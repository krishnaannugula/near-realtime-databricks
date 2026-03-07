// index.js (or whatever .js name you prefer)
module.exports = async function (context, mySbMsg) {
    context.log('JavaScript ServiceBus topic trigger function processed message', mySbMsg);

    const backendUrl = process.env.backend_url_logicapp;
    if (!backendUrl) {
        context.log.error('Backend URL is not defined in the environment variable');
        return;
    }

    try {
        const response = await fetch(backendUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(mySbMsg)
        });

        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status} ${response.statusText}`);
        }

        context.log('Message successfully forwarded to Logic app:', response.status, response.statusText);
    } catch (error) {
        context.log.error('Error forwarding message to Logic app:', error.message);
    }
};
