
// ARQUIVO SERVER.JS
// Substitua apenas o bloco do webhook GET pelo abaixo:

app.get('/api/webhook', (req, res) => {
    const token =
        process.env.META_WEBHOOK_VERIFY_TOKEN ||
        'peoople_token';

    if (req.query['hub.verify_token'] === token) {
        return res.status(200).send(req.query['hub.challenge']);
    }

    return res.sendStatus(403);
});

// O restante do seu server.js permanece igual.
