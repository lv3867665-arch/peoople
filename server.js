const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const axios = require('axios');
const cors = require('cors');
const compression = require('compression');
const path = require('path');
const FormData = require('form-data');
require('dotenv').config();

const app = express();
const server = http.createServer(app);

// ============================================
// SOCKET.IO
// ============================================
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] },
    pingTimeout: 10000,
    pingInterval: 5000,
    transports: ['websocket', 'polling'],
    upgradeTimeout: 5000
});

// ============================================
// MIDDLEWARES
// ============================================
app.use(compression());
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static(path.join(__dirname)));

let templateCache = { data: [], lastUpdate: 0 };

// ============================================
// CACHE DE CONFIG — evita query ao banco em cada mensagem
// ============================================
let _configCache = null;
let _configCacheTime = 0;
const CONFIG_CACHE_TTL = 60000; // 1 minuto

async function getConfig() {
    const now = Date.now();
    if (_configCache && (now - _configCacheTime) < CONFIG_CACHE_TTL) {
        return _configCache;
    }
    _configCache = await Config.findOne().lean();
    _configCacheTime = now;
    return _configCache;
}

// ============================================
// HEALTHCHECK
// ============================================
app.get('/health', (req, res) => {
    res.status(200).json({ 
        status: 'ok', 
        timestamp: new Date().toISOString(),
        mongoConnected: mongoose.connection.readyState === 1
    });
});

// ============================================
// FUNÇÃO: Normalização de Telefone
// ============================================
function normalizePhone(phone) {
    if (!phone) return phone;
    let clean = phone.replace(/\D/g, '');
    if (clean.startsWith('0')) clean = clean.slice(1);
    if (clean.length === 10 || clean.length === 11) clean = '55' + clean;
    if (clean.startsWith('55') && clean.length === 13 && clean[4] === '9') {
        return clean.slice(0, 4) + clean.slice(5);
    }
    return clean;
}

// ============================================
// MONGODB ATLAS - Conexão Otimizada
// ============================================

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/peoople-crm';

function getMongoURI() {
    let uri = MONGODB_URI;
    const separator = uri.includes('?') ? '&' : '?';
    const options = [
        'retryWrites=true',
        'w=majority',
        'maxPoolSize=10',
        'serverSelectionTimeoutMS=30000',
        'socketTimeoutMS=45000',
        'connectTimeoutMS=30000',
        'heartbeatFrequencyMS=10000',
        'tls=true',
        'tlsAllowInvalidCertificates=false'
    ];
    for (const opt of options) {
        const key = opt.split('=')[0];
        if (!uri.includes(key + '=')) {
            uri += separator + opt;
        }
    }
    return uri;
}

console.log('🔌 Iniciando conexão MongoDB Atlas...');

const mongoURI = getMongoURI();
console.log('URI (oculta):', mongoURI.replace(/\/\/[^:]+:[^@]+@/, '//***:***@'));

const connectWithRetry = async (retries = 5, delay = 5000) => {
    for (let i = 0; i < retries; i++) {
        try {
            await mongoose.connect(mongoURI, {
                useNewUrlParser: true,
                useUnifiedTopology: true
            });
            console.log('✅ MongoDB Atlas Conectado!');
            return true;
        } catch (err) {
            console.error(`❌ Tentativa ${i + 1}/${retries}:`, err.message);
            if (i < retries - 1) {
                console.log(`⏳ Retry em ${delay}ms...`);
                await new Promise(r => setTimeout(r, delay));
            }
        }
    }
    console.error('❌ Falha permanente na conexão MongoDB');
    return false;
};

connectWithRetry().then(connected => {
    if (!connected) {
        console.log('⚠️  Servidor iniciando sem MongoDB (modo degradado)');
    }
});

mongoose.connection.on('connected', () => console.log('🟢 MongoDB: Connected'));
mongoose.connection.on('error', (err) => console.error('🔴 MongoDB Error:', err.message));
mongoose.connection.on('disconnected', () => console.warn('🟡 MongoDB: Disconnected'));

// ============================================
// SCHEMAS
// ============================================

const ConfigSchema = new mongoose.Schema({
    profileName: String,
    profilePicture: String,
    metaAccessToken: String,
    metaPhoneNumberId: String,
    metaBusinessAccountId: String,
    metaWebhookVerifyToken: String,
    tagColors: { type: Map, of: String, default: {} }
}, { minimize: false });

const LeadSchema = new mongoose.Schema({
    name: String,
    phone: { type: String, unique: true, index: true },
    tags: [String],
    lastInteraction: { type: Date, default: Date.now, index: true },
    updatedAt: { type: Date, default: Date.now, index: true }
}, { timestamps: true });

const MessageSchema = new mongoose.Schema({
    phone: { type: String, required: true, index: true },
    text: { type: String, default: '' },
    direction: { type: String, enum: ['sent', 'received'], required: true, index: true },
    timestamp: { type: Date, default: Date.now, required: true, index: true },
    type: { type: String, default: 'text' },
    mediaData: { type: String, default: null },
    status: { type: String, default: 'sent', index: true },
    wamid: { type: String, index: true, sparse: true },
    fileName: { type: String, default: null },
    error: { type: String, default: null },
    locationData: {
        lat: { type: Number, default: null },
        lng: { type: Number, default: null },
        name: { type: String, default: null },
        address: { type: String, default: null }
    }
});

MessageSchema.index({ phone: 1, timestamp: -1 });

const QuickReplySchema = new mongoose.Schema({
    shortcut: String,
    text: String,
    mediaData: String,
    mediaName: String,
    mediaType: String
});

const Config = mongoose.model('Config', ConfigSchema);
const Lead = mongoose.model('Lead', LeadSchema);
const Message = mongoose.model('Message', MessageSchema);
const QuickReply = mongoose.model('QuickReply', QuickReplySchema);

// ============================================
// SOCKET.IO
// ============================================
io.on('connection', (socket) => {
    console.log('Cliente:', socket.id);
    
    socket.on('joinChat', (phone) => {
        const normalized = normalizePhone(phone);
        socket.rooms.forEach(r => { if (r !== socket.id) socket.leave(r); });
        socket.join(normalized);
    });
    
    socket.on('disconnect', () => {});
});

// ============================================
// WEBHOOK META
// ============================================
app.get('/api/webhook', (req, res) => {
    const token =
        process.env.META_WEBHOOK_VERIFY_TOKEN ||
        'peoople_token';

    if (req.query['hub.verify_token'] === token) {
        return res.status(200).send(req.query['hub.challenge']);
    }

    return res.sendStatus(403);
});

app.post('/api/webhook', async (req, res) => {
    res.sendStatus(200);
    
    setImmediate(async () => {
        try {
            const entry = req.body.entry?.[0];
            const changes = entry?.changes?.[0]?.value;
            if (!changes) return;
            
            // Usa cache — não vai ao banco em cada mensagem
            const config = await getConfig();
            if (!config?.metaAccessToken) return;

            // Atualização de status
            if (changes?.statuses?.[0]) {
                const status = changes.statuses[0];
                const metaError = status.errors?.[0];
                // Monta a mensagem de erro legível: código + título + detalhe
                const errorText = metaError
                    ? `[${metaError.code}] ${metaError.title || ''}${metaError.error_data?.details ? ' — ' + metaError.error_data.details : ''}`
                    : null;

                const update = { status: status.status };
                if (errorText) update.error = errorText;

                const msg = await Message.findOneAndUpdate(
                    { wamid: status.id },
                    update,
                    { new: true, lean: true }
                );
                if (msg) io.to(msg.phone).emit('statusUpdate', {
                    wamid: status.id,
                    status: status.status,
                    error: errorText || null
                });
            }

            // Nova mensagem recebida
            if (changes?.messages?.[0]) {
                const msg = changes.messages[0];
                const contact = changes.contacts?.[0];
                let phone = normalizePhone(msg.from);
                let text = msg.text?.body || "";
                let type = msg.type || 'text';
                let fileName = null;
                let locationData = null;
                const isMedia = ['image', 'audio', 'video', 'document'].includes(type);

                if (isMedia) {
                    fileName = msg[type]?.filename || null;
                    text = `[${type.toUpperCase()}]`;
                }

                if (type === 'location' && msg.location) {
                    locationData = {
                        lat: msg.location.latitude,
                        lng: msg.location.longitude,
                        name: msg.location.name || null,
                        address: msg.location.address || null
                    };
                    const label = locationData.name || locationData.address || null;
                    text = label ? `📍 ${label}` : '📍 Localização compartilhada';
                }

                // SALVA E EMITE IMEDIATAMENTE — sem esperar o download da mídia
                const savedMsg = await Message.create({
                    phone, text, direction: 'received', type,
                    mediaData: null, wamid: msg.id, fileName,
                    locationData: locationData || undefined
                });

                let lead = await Lead.findOneAndUpdate(
                    { phone },
                    { updatedAt: new Date() },
                    { new: true, upsert: true, lean: true }
                );
                
                if (!lead.name && contact?.profile?.name) {
                    lead = await Lead.findOneAndUpdate(
                        { phone },
                        { name: contact.profile.name },
                        { new: true, lean: true }
                    );
                }

                // Emite para o cliente IMEDIATAMENTE (a mídia chega depois)
                io.to(phone).emit('newMessage', {
                    _id: savedMsg._id, phone: savedMsg.phone, text: savedMsg.text,
                    direction: savedMsg.direction, type: savedMsg.type,
                    mediaData: null, timestamp: savedMsg.timestamp,
                    status: savedMsg.status, wamid: savedMsg.wamid, fileName: savedMsg.fileName,
                    locationData: savedMsg.locationData || null
                });
                
                io.emit('conversationUpdate', {
                    phone, name: lead?.name || phone, lastMessage: text,
                    timestamp: savedMsg.timestamp, lastReceived: savedMsg.timestamp,
                    tags: lead?.tags || [], direction: 'received'
                });

                // Baixa a mídia em background e emite atualização separada
                if (isMedia) {
                    const mediaId = msg[type]?.id;
                    if (mediaId) {
                        setImmediate(async () => {
                            try {
                                const mediaRes = await axios.get(
                                    `https://graph.facebook.com/v18.0/${mediaId}`,
                                    { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 8000 }
                                );
                                const fileRes = await axios.get(
                                    mediaRes.data.url,
                                    { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, responseType: 'arraybuffer', timeout: 10000 }
                                );
                                const mimeType = type === 'audio' ? 'audio/ogg' : (type === 'document' ? 'application/pdf' : 'image/jpeg');
                                const mediaData = `data:${mimeType};base64,${Buffer.from(fileRes.data).toString('base64')}`;
                                
                                await Message.findByIdAndUpdate(savedMsg._id, { mediaData });
                                
                                // Emite a mídia quando estiver pronta — o frontend atualiza a mensagem
                                io.to(phone).emit('mediaUpdate', { _id: String(savedMsg._id), mediaData });
                            } catch (e) {
                                console.error('Erro download mídia:', e.message);
                            }
                        });
                    }
                }
            }
        } catch (e) {
            console.error('Erro webhook:', e);
        }
    });
});

// ============================================
// UPLOAD DE MÍDIA (para disparo em massa: faz upload uma vez, reutiliza mediaId)
// ============================================
app.post('/api/media/upload', async (req, res) => {
    try {
        const { mediaData, mediaType, fileName } = req.body;
        if (!mediaData) return res.status(400).json({ error: 'mediaData obrigatório' });
        const config = await getConfig();
        if (!config?.metaAccessToken) return res.status(400).json({ error: 'Config incompleta' });

        const buffer = Buffer.from(mediaData.split(',')[1], 'base64');
        const formData = new FormData();
        formData.append('file', buffer, { filename: fileName || 'file', contentType: mediaType });
        formData.append('messaging_product', 'whatsapp');

        const uploadRes = await axios.post(
            `https://graph.facebook.com/v18.0/${config.metaPhoneNumberId}/media`,
            formData,
            { headers: { ...formData.getHeaders(), Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 30000 }
        );
        res.json({ mediaId: uploadRes.data.id });
    } catch (e) {
        console.error('Erro upload mídia:', e.response?.data || e.message);
        res.status(500).json({ error: e.response?.data?.error?.message || e.message });
    }
});

// ============================================
// ENVIO DE MENSAGEM
// ============================================
app.post('/api/messages/send', async (req, res) => {
    try {
        let { phone, messageText, templateName, languageCode, mediaData, mediaType, fileName, mediaId, replyToWamid, lat, lng } = req.body;
        phone = normalizePhone(phone);
        
        // Usa cache — não vai ao banco em cada envio
        const config = await getConfig();
        if (!config?.metaAccessToken) return res.status(400).json({ error: 'Config incompleta' });

        // ── Envio de Localização ──────────────────────────────
        if (lat !== undefined && lng !== undefined) {
            const locationPayload = {
                messaging_product: "whatsapp",
                to: phone,
                type: "location",
                location: {
                    latitude: lat,
                    longitude: lng
                }
            };
            const metaRes = await axios.post(
                `https://graph.facebook.com/v18.0/${config.metaPhoneNumberId}/messages`,
                locationPayload,
                { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 10000 }
            );
            const wamid = metaRes.data.messages?.[0]?.id;
            res.json({ success: true, wamid });
            setImmediate(async () => {
                try {
                    const [savedMsg, lead] = await Promise.all([
                        Message.create({
                            phone,
                            text: '📍 Localização compartilhada',
                            direction: 'sent',
                            type: 'location',
                            wamid,
                            status: 'sent',
                            locationData: { lat, lng, name: null, address: null }
                        }),
                        Lead.findOneAndUpdate({ phone }, { updatedAt: new Date() }, { new: true, lean: true })
                    ]);
                    io.to(phone).emit('newMessage', { ...savedMsg.toObject(), locationData: savedMsg.locationData });
                    io.emit('conversationUpdate', { phone, name: lead?.name || phone, lastMessage: '📍 Localização compartilhada', timestamp: savedMsg.timestamp, direction: 'sent' });
                } catch (e) {
                    console.error('Erro ao salvar localização enviada:', e.message);
                }
            });
            return;
        }

        let payload = { messaging_product: "whatsapp", to: phone };
        if (replyToWamid) payload.context = { message_id: replyToWamid };
        let finalText = messageText || '';
        let finalType = 'text';

        if (templateName) {
            payload.type = "template";
            payload.template = { name: templateName, language: { code: languageCode || "pt_BR" } };

            // mediaId já carregado previamente (disparo em massa) ou carrega agora
            if (mediaId || mediaData) {
                let uploadedId = mediaId;
                if (!uploadedId) {
                    const buffer = Buffer.from(mediaData.split(',')[1], 'base64');
                    const formData = new FormData();
                    formData.append('file', buffer, { filename: fileName || 'file', contentType: mediaType });
                    formData.append('messaging_product', 'whatsapp');
                    const uploadRes = await axios.post(
                        `https://graph.facebook.com/v18.0/${config.metaPhoneNumberId}/media`,
                        formData,
                        { headers: { ...formData.getHeaders(), Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 20000 }
                    );
                    uploadedId = uploadRes.data.id;
                }
                const mediaTypeKey = mediaType?.includes('image') ? 'image' : (mediaType?.includes('pdf') ? 'document' : 'image');
                payload.template.components = [{ type: "header", parameters: [{ type: mediaTypeKey, [mediaTypeKey]: { id: uploadedId } }] }];
            }
        } else if (mediaData) {
            const buffer = Buffer.from(mediaData.split(',')[1], 'base64');
            const formData = new FormData();
            formData.append('file', buffer, { filename: fileName || 'file', contentType: mediaType });
            formData.append('messaging_product', 'whatsapp');

            const uploadRes = await axios.post(
                `https://graph.facebook.com/v18.0/${config.metaPhoneNumberId}/media`,
                formData,
                { headers: { ...formData.getHeaders(), Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 20000 }
            );

            finalType = mediaType?.includes('audio') ? 'audio' : (mediaType?.includes('pdf') ? 'document' : 'image');
            payload.type = finalType;
            payload[finalType] = { id: uploadRes.data.id };
            // Adiciona caption/filename sem sobrescrever o id do upload
            if (finalType === 'image' && messageText) payload.image.caption = messageText;
            if (finalType === 'document') payload.document = { id: uploadRes.data.id, filename: fileName || 'arquivo' };
            finalText = messageText || `[${finalType.toUpperCase()}]`;
        } else {
            payload.type = "text";
            payload.text = { body: messageText };
        }

        const metaRes = await axios.post(
            `https://graph.facebook.com/v18.0/${config.metaPhoneNumberId}/messages`,
            payload,
            { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 10000 }
        );

        const wamid = metaRes.data.messages?.[0]?.id;

        // Responde ao frontend imediatamente após Meta confirmar — não espera o banco
        res.json({ success: true, wamid });

        // Salva no banco e emite socket em background (não bloqueia a resposta)
        setImmediate(async () => {
            try {
                const [savedMsg, lead] = await Promise.all([
                    Message.create({
                        phone, text: finalText, direction: 'sent', type: finalType,
                        mediaData: mediaData || null, wamid,
                        status: 'sent', fileName
                    }),
                    Lead.findOneAndUpdate({ phone }, { updatedAt: new Date() }, { new: true, lean: true })
                ]);
                io.to(phone).emit('newMessage', savedMsg);
                io.emit('conversationUpdate', { phone, name: lead?.name || phone, lastMessage: finalText, timestamp: savedMsg.timestamp, direction: 'sent' });
            } catch (e) {
                console.error('Erro ao salvar msg enviada:', e.message);
            }
        });

    } catch (e) {
        console.error('Erro envio:', e.message);
        res.status(500).json({ error: e.message });
    }
});

// ============================================
// ROTAS DE CONVERSAS - SIMPLES E RÁPIDO
// ============================================

let conversationsCache = { data: null, timestamp: 0, ttl: 15000 };

app.get("/api/conversations", async (req, res) => {
    const startTime = Date.now();
    
    if (conversationsCache.data && (Date.now() - conversationsCache.timestamp) < conversationsCache.ttl) {
        return res.json(conversationsCache.data);
    }

    const timeoutId = setTimeout(() => {
        if (!res.headersSent) {
            console.warn('[Conversations] Timeout — retornando cache');
            res.json(conversationsCache.data || []);
        }
    }, 10000);

    try {
        if (mongoose.connection.readyState !== 1) {
            clearTimeout(timeoutId);
            return res.json(conversationsCache.data || []);
        }

        // Aggregation: última mensagem por telefone (sem limite de quantidade de conversas)
        const agg = await Message.aggregate([
            { $sort: { timestamp: -1 } },
            { $group: {
                _id: '$phone',
                lastMessage: { $first: '$text' },
                timestamp: { $first: '$timestamp' },
                lastReceived: {
                    $max: {
                        $cond: [{ $eq: ['$direction', 'received'] }, '$timestamp', null]
                    }
                }
            }},
            { $sort: { timestamp: -1 } }
        ]).option({ maxTimeMS: 8000 });

        clearTimeout(timeoutId);

        const phones = agg.map(a => a._id);

        let leadsMap = new Map();
        if (phones.length > 0) {
            try {
                const leads = await Lead.find({ phone: { $in: phones } })
                    .select('phone name tags')
                    .maxTimeMS(5000)
                    .lean();
                leadsMap = new Map(leads.map(l => [l.phone, l]));
            } catch (e) {
                console.error('[Conversations] Erro leads:', e.message);
            }
        }

        const result = agg.map(c => {
            const lead = leadsMap.get(c._id);
            return {
                phone: c._id,
                name: lead?.name || c._id,
                lastMessage: c.lastMessage || '',
                timestamp: c.timestamp,
                lastReceived: c.lastReceived || null,
                tags: lead?.tags || []
            };
        });

        conversationsCache = { data: result, timestamp: Date.now(), ttl: 15000 };
        console.log(`[Conversations] ${result.length} conversas em ${Date.now() - startTime}ms`);
        res.json(result);

    } catch (error) {
        clearTimeout(timeoutId);
        console.error('[Conversations] ERRO:', error.message);
        res.json(conversationsCache.data || []);
    }
});

// ============================================
// ROTAS DE MENSAGENS
// ============================================

// Retorna só o campo mediaData de uma mensagem — para o frontend verificar se mídia já baixou
app.get('/api/messages/media/:id', async (req, res) => {
    try {
        const msg = await Message.findById(req.params.id).select('mediaData type').lean();
        if (!msg) return res.status(404).json({ error: 'Mensagem não encontrada' });
        res.json({ mediaData: msg.mediaData, type: msg.type });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/api/messages/:phone', async (req, res) => {
    try {
        const normalizedPhone = normalizePhone(req.params.phone);
        const limit = parseInt(req.query.limit) || 50;
        const before = req.query.before;

        const query = { phone: normalizedPhone };
        if (before) query.timestamp = { $lt: new Date(before) };

        // Exclui mediaData do retorno — pode ser centenas de KB por mensagem
        // O frontend carrega a mídia separadamente via /api/messages/media/:id
        const messages = await Message.find(query)
            .select('-mediaData')
            .sort({ timestamp: -1 })
            .limit(limit)
            .lean();

        // Adiciona flag hasMedia para o frontend saber quais mensagens têm mídia no banco
        const result = messages.reverse().map(m => ({
            ...m,
            hasMedia: ['image', 'audio', 'video', 'document'].includes(m.type),
            mediaData: null,
            locationData: m.locationData || null
        }));

        res.json(result);

        setImmediate(() => {
            Message.updateMany(
                { phone: normalizedPhone, direction: 'received', status: { $ne: 'read' } },
                { status: 'read' }
            ).exec();
        });
        
    } catch (e) {
        console.error('Erro messages:', e);
        res.status(500).json({ error: e.message });
    }
});

// ============================================
// ROTAS DE LEADS
// ============================================
app.get('/api/leads', async (req, res) => {
    try {
        const leads = await Lead.find().sort({ updatedAt: -1 }).limit(200).lean();
        res.json(leads);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post("/api/leads", async (req, res) => {
    try {
        const phone = normalizePhone(req.body.phone);
        const lead = await Lead.findOneAndUpdate(
            { phone },
            { name: req.body.name, phone, tags: req.body.tags || [], updatedAt: new Date() },
            { upsert: true, new: true, lean: true }
        );
        io.emit('leadUpdate', lead);
        res.json(lead);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/leads/:phone', async (req, res) => {
    try {
        const phone = normalizePhone(req.params.phone);
        await Lead.findOneAndDelete({ phone });
        io.emit('leadDelete', { phone });
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ============================================
// ROTAS DE QUICK REPLIES
// ============================================
app.get('/api/quick-replies', async (req, res) => {
    try {
        const replies = await QuickReply.find().lean();
        res.json(replies);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/quick-replies', async (req, res) => {
    try {
        const reply = await QuickReply.create(req.body);
        res.json(reply);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/quick-replies/:id', async (req, res) => {
    try {
        await QuickReply.findByIdAndDelete(req.params.id);
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ============================================
// ROTAS DE CONFIG
// ============================================
app.get('/api/config', async (req, res) => {
    try {
        const config = await getConfig() || {};
        res.json(config);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.post('/api/config', async (req, res) => {
    try {
        const config = await Config.findOneAndUpdate({}, req.body, { upsert: true, new: true, lean: true });
        // Atualiza o cache imediatamente ao salvar
        _configCache = config;
        _configCacheTime = Date.now();
        res.json(config);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ============================================
// TEMPLATES META
// ============================================
app.get('/api/meta/templates', async (req, res) => {
    try {
        const config = await getConfig();
        if (!config?.metaAccessToken) return res.json([]);

        if (templateCache.data.length > 0 && (Date.now() - templateCache.lastUpdate < 300000)) {
            return res.json(templateCache.data);
        }

        const response = await axios.get(
            `https://graph.facebook.com/v18.0/${config.metaBusinessAccountId}/message_templates`,
            { headers: { Authorization: `Bearer ${config.metaAccessToken}` }, timeout: 8000 }
        );

        templateCache.data = response.data.data || [];
        templateCache.lastUpdate = Date.now();
        res.json(templateCache.data);
        
    } catch (e) {
        res.json(templateCache.data || []);
    }
});

// ============================================
// SHUTDOWN
// ============================================
process.on('SIGTERM', () => {
    server.close(() => {
        mongoose.connection.close(false, () => process.exit(0));
    });
});

// ============================================
// INICIALIZAÇÃO
// ============================================
const PORT = process.env.PORT || 8080;

server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Servidor na porta ${PORT}`);
});
