/**
 * Serverless Event-Driven Processing Pipeline - Local Cloud Runner
 * 
 * Simulates:
 * 1. API Gateway (POST /upload)
 * 2. Amazon S3 Bucket (local_cloud/s3_bucket/)
 * 3. AWS Lambda Function (Event-Driven Trigger on S3 ObjectCreated)
 * 4. Amazon DynamoDB (local_cloud/dynamodb/ImageMetadataTable.json)
 * 5. Amazon SNS Topic (local_cloud/sns/MeraAlertSystem_notifications.json)
 * 6. Real Email Delivery (Nodemailer Gmail SMTP)
 * 7. Live Architecture Visualizer Dashboard (/dashboard)
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

let nodemailer = null;
try {
    nodemailer = require('nodemailer');
} catch (e) {
    console.warn("nodemailer not installed, real email delivery disabled");
}

const PORT = 3000;
const BASE_DIR = __dirname;
const CLOUD_DIR = path.join(BASE_DIR, 'local_cloud');
const S3_DIR = path.join(CLOUD_DIR, 's3_bucket');
const DYNAMO_DIR = path.join(CLOUD_DIR, 'dynamodb');
const SNS_DIR = path.join(CLOUD_DIR, 'sns');
const LOGS_DIR = path.join(CLOUD_DIR, 'logs');
const CONFIG_FILE = path.join(BASE_DIR, 'email_config.json');

const DYNAMO_TABLE_FILE = path.join(DYNAMO_DIR, 'ImageMetadataTable.json');
const SNS_NOTIFICATIONS_FILE = path.join(SNS_DIR, 'MeraAlertSystem_notifications.json');
const PIPELINE_LOGS_FILE = path.join(LOGS_DIR, 'pipeline_events.log');

// Ensure directories exist
[CLOUD_DIR, S3_DIR, DYNAMO_DIR, SNS_DIR, LOGS_DIR].forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

if (!fs.existsSync(DYNAMO_TABLE_FILE)) fs.writeFileSync(DYNAMO_TABLE_FILE, JSON.stringify([], null, 2));
if (!fs.existsSync(SNS_NOTIFICATIONS_FILE)) fs.writeFileSync(SNS_NOTIFICATIONS_FILE, JSON.stringify([], null, 2));

function getEmailConfig() {
    if (fs.existsSync(CONFIG_FILE)) {
        try {
            return JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
        } catch (e) {}
    }
    return { enabled: false, service: 'gmail', senderEmail: '', senderAppPassword: '', recipientEmail: '' };
}

function logPipeline(stage, message, data = null) {
    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] [${stage.toUpperCase()}] ${message}${data ? ' ' + JSON.stringify(data) : ''}\n`;
    console.log(`\x1b[36m[${stage.toUpperCase()}]\x1b[0m ${message}`);
    fs.appendFileSync(PIPELINE_LOGS_FILE, logEntry);
}

// ----------------------------------------------------
// Real Email Sender via Nodemailer
// ----------------------------------------------------
async function sendRealEmail(subject, textContent) {
    const config = getEmailConfig();
    if (!config.enabled || !config.senderEmail || !config.senderAppPassword || !config.recipientEmail) {
        logPipeline('Email', 'ℹ️ Real Email is paused or credentials not set. Set your email in email_config.json or Dashboard!');
        return { success: false, reason: 'Credentials not configured' };
    }

    if (!nodemailer) {
        logPipeline('Email', '❌ Nodemailer library missing.');
        return { success: false, reason: 'Nodemailer missing' };
    }

    try {
        const transporter = nodemailer.createTransport({
            service: config.service || 'gmail',
            auth: {
                user: config.senderEmail,
                pass: config.senderAppPassword.replace(/\s+/g, '') // remove spaces from Google app passwords
            }
        });

        const info = await transporter.sendMail({
            from: `"Cloud Pipeline Alert" <${config.senderEmail}>`,
            to: config.recipientEmail,
            subject: subject,
            text: textContent,
            html: `
                <div style="font-family: Arial, sans-serif; padding: 20px; border: 1px solid #e2e8f0; border-radius: 8px; max-width: 600px;">
                    <h2 style="color: #0284c7; margin-top: 0;">☁️ AWS Cloud Pipeline Notification</h2>
                    <p style="font-size: 16px; color: #334155;">${textContent}</p>
                    <hr style="border: 0; border-top: 1px solid #e2e8f0; margin: 20px 0;">
                    <p style="font-size: 12px; color: #64748b;">
                        <b>Source:</b> Serverless Event-Driven Pipeline<br>
                        <b>Topic:</b> arn:aws:sns:eu-north-1:564208780370:MeraAlertSystem<br>
                        <b>Time:</b> ${new Date().toLocaleString()}
                    </p>
                </div>
            `
        });

        logPipeline('Email', `📬 REAL EMAIL SENT successfully to ${config.recipientEmail}! (MessageId: ${info.messageId})`);
        return { success: true, messageId: info.messageId };
    } catch (err) {
        logPipeline('Email', `❌ Real Email Delivery Failed: ${err.message}`);
        return { success: false, error: err.message };
    }
}

// ----------------------------------------------------
// AWS Lambda Function Logic
// ----------------------------------------------------
async function executeLambdaFunction(event) {
    logPipeline('Lambda', '⚡ AWS Lambda Triggered by S3 Event!');
    
    const record = event.Records[0];
    const bucketName = record.s3.bucket.name;
    const fileName = record.s3.object.key;
    const fileSize = record.s3.object.size;
    
    const metadata = {
        'ImageID': crypto.randomUUID(),
        'FileName': fileName,
        'BucketName': bucketName,
        'FileSizeInBytes': fileSize,
        'ProcessedAt': new Date().toISOString()
    };
    logPipeline('DynamoDB', '📥 Saving metadata into DynamoDB Table: ImageMetadataTable', metadata);
    
    let tableItems = [];
    try { tableItems = JSON.parse(fs.readFileSync(DYNAMO_TABLE_FILE, 'utf8')); } catch (e) { tableItems = []; }
    tableItems.unshift(metadata);
    fs.writeFileSync(DYNAMO_TABLE_FILE, JSON.stringify(tableItems, null, 2));

    const snsNotification = {
        notificationId: crypto.randomUUID(),
        topicArn: 'arn:aws:sns:eu-north-1:564208780370:MeraAlertSystem',
        subject: 'Cloud Pipeline Notification',
        message: `Success! Your file ${fileName} has been securely processed and stored.`,
        sentAt: new Date().toISOString(),
        metadataRef: metadata.ImageID
    };
    logPipeline('SNS', `📧 Alert Published to SNS Topic: MeraAlertSystem -> "${snsNotification.message}"`);

    let snsList = [];
    try { snsList = JSON.parse(fs.readFileSync(SNS_NOTIFICATIONS_FILE, 'utf8')); } catch (e) { snsList = []; }
    snsList.unshift(snsNotification);
    fs.writeFileSync(SNS_NOTIFICATIONS_FILE, JSON.stringify(snsList, null, 2));

    // Real Email Trigger
    const emailResult = await sendRealEmail(snsNotification.subject, snsNotification.message);

    return {
        statusCode: 200,
        body: 'Metadata logged and Email Sent!',
        metadata,
        snsNotification,
        realEmailStatus: emailResult
    };
}

// ----------------------------------------------------
// HTTP Server
// ----------------------------------------------------
const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }

    const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
    const pathname = parsedUrl.pathname;

    // Route 1: Serve Frontend
    if (pathname === '/' || pathname === '/index.html') {
        const filePath = path.join(BASE_DIR, 'frontend', 'index.html');
        fs.readFile(filePath, (err, content) => {
            if (err) {
                res.writeHead(500, { 'Content-Type': 'text/plain' });
                res.end('Error loading frontend');
            } else {
                res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
                res.end(content);
            }
        });
        return;
    }

    // Route 2: API Gateway POST /upload
    if (pathname === '/upload' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', async () => {
            try {
                const data = JSON.parse(body);
                const fileName = data.filename || `upload-${Date.now()}.png`;
                const base64Data = data.data;

                if (!base64Data) {
                    res.writeHead(400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'No image data provided' }));
                    return;
                }

                const buffer = Buffer.from(base64Data, 'base64');
                const s3Key = `${Date.now()}_${fileName.replace(/\s+/g, '_')}`;
                const filePath = path.join(S3_DIR, s3Key);
                fs.writeFileSync(filePath, buffer);

                logPipeline('S3', `🪣 Object Created in S3 Bucket [my-cloud-storage-bucket]: ${s3Key} (${buffer.length} bytes)`);

                const s3Event = {
                    Records: [{
                        eventVersion: '2.1',
                        eventSource: 'aws:s3',
                        awsRegion: 'eu-north-1',
                        eventTime: new Date().toISOString(),
                        eventName: 'ObjectCreated:Put',
                        s3: {
                            s3SchemaVersion: '1.0',
                            configurationId: 'LocalCloudConfigRule',
                            bucket: {
                                name: 'my-cloud-storage-bucket',
                                arn: 'arn:aws:s3:::my-cloud-storage-bucket'
                            },
                            object: { key: s3Key, size: buffer.length }
                        }
                    }]
                };

                const lambdaResult = await executeLambdaFunction(s3Event);

                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({
                    success: true,
                    message: 'Mubarak ho! Photo AWS S3 me pahunch gayi aur Lambda chal gaya! 🎉',
                    s3Key,
                    fileSize: buffer.length,
                    lambdaResult
                }));
            } catch (err) {
                console.error('Upload error:', err);
                res.writeHead(500, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Internal server error', details: err.message }));
            }
        });
        return;
    }

    // Route 3: Live Cloud Status API
    if (pathname === '/api/cloud-status' && req.method === 'GET') {
        const s3Files = fs.readdirSync(S3_DIR).map(f => {
            const stat = fs.statSync(path.join(S3_DIR, f));
            return { name: f, size: stat.size, createdAt: stat.birthtime };
        }).sort((a, b) => b.createdAt - a.createdAt);

        let dynamoItems = [];
        try { dynamoItems = JSON.parse(fs.readFileSync(DYNAMO_TABLE_FILE, 'utf8')); } catch (e) {}

        let snsNotifications = [];
        try { snsNotifications = JSON.parse(fs.readFileSync(SNS_NOTIFICATIONS_FILE, 'utf8')); } catch (e) {}

        const emailConfig = getEmailConfig();

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            s3Files,
            dynamoItems,
            snsNotifications,
            emailConfig: {
                enabled: !!emailConfig.enabled,
                senderEmail: emailConfig.senderEmail,
                recipientEmail: emailConfig.recipientEmail,
                hasPassword: !!emailConfig.senderAppPassword
            }
        }));
        return;
    }

    // Route 4: Save Email Config API
    if (pathname === '/api/save-email-config' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => { body += chunk.toString(); });
        req.on('end', () => {
            try {
                const newConfig = JSON.parse(body);
                const current = getEmailConfig();
                const merged = {
                    enabled: true,
                    service: newConfig.service || 'gmail',
                    senderEmail: newConfig.senderEmail || current.senderEmail,
                    senderAppPassword: newConfig.senderAppPassword || current.senderAppPassword,
                    recipientEmail: newConfig.recipientEmail || current.recipientEmail
                };
                fs.writeFileSync(CONFIG_FILE, JSON.stringify(merged, null, 2));
                logPipeline('Config', '⚙️ Email configuration updated!');
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: true, message: 'Settings saved!' }));
            } catch (err) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: err.message }));
            }
        });
        return;
    }

    // Route 5: Send Test Email API
    if (pathname === '/api/send-test-email' && req.method === 'POST') {
        sendRealEmail(
            "Test Alert: AWS Cloud Pipeline",
            "Yeh ek test email hai. Aapka Cloud Event-Driven Pipeline ab real email bhejne ke liye bilkul tayyar hai!"
        ).then(result => {
            res.writeHead(result.success ? 200 : 400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(result));
        });
        return;
    }

    // Route 6: Serve uploaded S3 image files
    if (pathname.startsWith('/s3-files/')) {
        const fileName = path.basename(pathname);
        const filePath = path.join(S3_DIR, fileName);
        if (fs.existsSync(filePath)) {
            res.writeHead(200, { 'Content-Type': 'image/jpeg' });
            fs.createReadStream(filePath).pipe(res);
            return;
        }
    }

    // Route 7: Visual Architecture Dashboard
    if (pathname === '/dashboard') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(renderDashboardHtml());
        return;
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not Found');
});

function renderDashboardHtml() {
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AWS Cloud Pipeline - Live Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #0f172a;
            --card-bg: #1e293b;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --accent-s3: #38bdf8;
            --accent-lambda: #f59e0b;
            --accent-dynamo: #34d399;
            --accent-sns: #ec4899;
            --border-color: #334155;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Inter', sans-serif; background-color: var(--bg); color: var(--text-main); padding: 24px; }
        header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border-color); padding-bottom: 16px; margin-bottom: 24px; }
        h1 { font-size: 1.6rem; font-weight: 800; display: flex; align-items: center; gap: 10px; }
        .badge { background: #10b98122; color: #10b981; padding: 4px 12px; border-radius: 9999px; font-size: 0.85rem; font-weight: 600; border: 1px solid #10b98144; }
        .nav-links a { color: var(--accent-s3); text-decoration: none; font-weight: 600; margin-left: 15px; padding: 8px 16px; background: var(--card-bg); border-radius: 6px; border: 1px solid var(--border-color); }
        .nav-links a:hover { background: #334155; }
        
        .flow-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .flow-step { background: var(--card-bg); border: 1px solid var(--border-color); padding: 16px; border-radius: 10px; transition: transform 0.2s; }
        .flow-step:hover { transform: translateY(-2px); }
        .flow-step.s3 { border-top: 4px solid var(--accent-s3); }
        .flow-step.lambda { border-top: 4px solid var(--accent-lambda); }
        .flow-step.dynamo { border-top: 4px solid var(--accent-dynamo); }
        .flow-step.sns { border-top: 4px solid var(--accent-sns); }
        .step-title { font-size: 0.95rem; color: var(--text-muted); margin-bottom: 6px; }
        .step-val { font-size: 1.4rem; font-weight: 700; }

        .email-config-box {
            background: linear-gradient(135deg, #1e293b, #0f172a);
            border: 1px solid #ec489988;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 24px;
        }
        .email-form-grid {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr auto auto;
            gap: 12px;
            align-items: end;
            margin-top: 14px;
        }
        @media (max-width: 1000px) {
            .email-form-grid { grid-template-columns: 1fr; }
        }
        .form-group label { display: block; font-size: 0.8rem; color: var(--text-muted); margin-bottom: 6px; font-weight: 600; }
        .form-group input { width: 100%; background: #0b1120; border: 1px solid var(--border-color); color: #fff; padding: 10px; border-radius: 6px; font-size: 0.85rem; }
        .btn-primary { background: #ec4899; color: white; border: none; padding: 10px 18px; border-radius: 6px; font-weight: 600; cursor: pointer; height: 42px; }
        .btn-primary:hover { background: #db2777; }
        .btn-secondary { background: #334155; color: white; border: none; padding: 10px 18px; border-radius: 6px; font-weight: 600; cursor: pointer; height: 42px; }
        .btn-secondary:hover { background: #475569; }

        .content-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
        @media (max-width: 900px) { .content-grid { grid-template-columns: 1fr; } }
        .panel { background: var(--card-bg); border: 1px solid var(--border-color); border-radius: 12px; padding: 20px; }
        .panel-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; border-bottom: 1px solid var(--border-color); padding-bottom: 10px; }
        .panel-header h2 { font-size: 1.15rem; font-weight: 700; display: flex; align-items: center; gap: 8px; }
        table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
        th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--border-color); }
        th { color: var(--text-muted); font-weight: 600; }
        tr:hover { background-color: #243447; }
        .empty-state { text-align: center; color: var(--text-muted); padding: 24px 0; font-style: italic; }
        .email-card { background: #152238; border-left: 3px solid var(--accent-sns); padding: 12px; margin-bottom: 12px; border-radius: 6px; }
        .email-subject { font-weight: 700; color: #f472b6; margin-bottom: 4px; font-size: 0.9rem; }
        .email-body { font-size: 0.85rem; color: #cbd5e1; }
        .email-time { font-size: 0.75rem; color: var(--text-muted); margin-top: 6px; }
        .img-thumb { width: 42px; height: 42px; object-fit: cover; border-radius: 4px; border: 1px solid var(--border-color); }
    </style>
</head>
<body>

    <header>
        <div>
            <h1>☁️ AWS Serverless Pipeline Simulator</h1>
            <p style="color: var(--text-muted); font-size: 0.9rem; margin-top: 4px;">Local Cloud Architecture: API Gateway ➔ S3 ➔ Lambda ➔ DynamoDB ➔ SNS</p>
        </div>
        <div class="nav-links">
            <span class="badge">● Simulator Running</span>
            <a href="/" target="_blank">🚀 Open Uploader</a>
        </div>
    </header>

    <!-- Email Configuration Box -->
    <div class="email-config-box">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <h3 style="color: #f472b6; font-size: 1.1rem; display: flex; align-items: center; gap: 8px;">
                📬 Real Email Delivery Setup (Gmail SMTP)
            </h3>
            <span id="email-status-tag" style="font-size: 0.8rem; background: #334155; padding: 3px 8px; border-radius: 4px; color: #cbd5e1;">Checking status...</span>
        </div>
        <p style="color: var(--text-muted); font-size: 0.85rem; margin-top: 6px;">
            Yahan apna Gmail aur 16-digit <b>Google App Password</b> daalein taaki upload hote hi aapke <b>asli Email Inbox</b> me turant mail aaye!
        </p>
        <div class="email-form-grid">
            <div class="form-group">
                <label>Sender Gmail (Bhejne wala)</label>
                <input type="email" id="cfg-sender" placeholder="example@gmail.com">
            </div>
            <div class="form-group">
                <label>Google App Password (16-char)</label>
                <input type="password" id="cfg-pass" placeholder="xxxx xxxx xxxx xxxx">
            </div>
            <div class="form-group">
                <label>Recipient Email (Jahan email chahiye)</label>
                <input type="email" id="cfg-recipient" placeholder="apna-email@gmail.com">
            </div>
            <button class="btn-primary" onclick="saveEmailConfig()">💾 Save Config</button>
            <button class="btn-secondary" onclick="sendTestEmail()">📨 Send Test</button>
        </div>
        <div id="email-action-msg" style="margin-top: 10px; font-size: 0.85rem;"></div>
    </div>

    <div class="flow-container">
        <div class="flow-step s3">
            <div class="step-title">🪣 Amazon S3 Objects</div>
            <div class="step-val" id="s3-count">0</div>
            <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 4px;">Bucket: my-cloud-storage-bucket</div>
        </div>
        <div class="flow-step lambda">
            <div class="step-title">⚡ Lambda Executions</div>
            <div class="step-val" id="lambda-count">0</div>
            <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 4px;">Trigger: s3:ObjectCreated:*</div>
        </div>
        <div class="flow-step dynamo">
            <div class="step-title">🗄️ DynamoDB Records</div>
            <div class="step-val" id="dynamo-count">0</div>
            <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 4px;">Table: ImageMetadataTable</div>
        </div>
        <div class="flow-step sns">
            <div class="step-title">📩 SNS Email Alerts</div>
            <div class="step-val" id="sns-count">0</div>
            <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 4px;">Topic: MeraAlertSystem</div>
        </div>
    </div>

    <div class="content-grid">
        <!-- DynamoDB Table Viewer -->
        <div class="panel">
            <div class="panel-header">
                <h2>🗄️ Amazon DynamoDB: ImageMetadataTable</h2>
            </div>
            <div style="overflow-x: auto;">
                <table>
                    <thead>
                        <tr>
                            <th>ImageID (Partition Key)</th>
                            <th>FileName</th>
                            <th>Size</th>
                            <th>Processed At</th>
                        </tr>
                    </thead>
                    <tbody id="dynamo-tbody">
                        <tr><td colspan="4" class="empty-state">No metadata recorded yet.</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- SNS Notifications Stream -->
        <div class="panel">
            <div class="panel-header">
                <h2>📩 Amazon SNS Alerts: MeraAlertSystem</h2>
            </div>
            <div id="sns-container">
                <div class="empty-state">No emails triggered yet.</div>
            </div>
        </div>
    </div>

    <div class="panel" style="margin-top: 24px;">
        <div class="panel-header">
            <h2>🪣 S3 Bucket Storage: my-cloud-storage-bucket</h2>
        </div>
        <div style="overflow-x: auto;">
            <table>
                <thead>
                    <tr>
                        <th>Preview</th>
                        <th>File Name (Key)</th>
                        <th>Size</th>
                        <th>Uploaded Time</th>
                    </tr>
                </thead>
                <tbody id="s3-tbody">
                    <tr><td colspan="4" class="empty-state">Bucket is empty.</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        let configLoaded = false;
        async function fetchStatus() {
            try {
                const res = await fetch('/api/cloud-status');
                const data = await res.json();

                // Counters
                document.getElementById('s3-count').innerText = data.s3Files.length;
                document.getElementById('lambda-count').innerText = data.dynamoItems.length;
                document.getElementById('dynamo-count').innerText = data.dynamoItems.length;
                document.getElementById('sns-count').innerText = data.snsNotifications.length;

                // Email config status
                const tag = document.getElementById('email-status-tag');
                if (data.emailConfig && data.emailConfig.hasPassword && data.emailConfig.senderEmail) {
                    tag.innerHTML = '🟢 Real Email Active (' + data.emailConfig.recipientEmail + ')';
                    tag.style.background = '#10b98133';
                    tag.style.color = '#10b981';
                } else {
                    tag.innerHTML = '🟡 Real Email Inactive (Config needed)';
                    tag.style.background = '#f59e0b33';
                    tag.style.color = '#f59e0b';
                }

                if (!configLoaded && data.emailConfig) {
                    if (data.emailConfig.senderEmail) document.getElementById('cfg-sender').value = data.emailConfig.senderEmail;
                    if (data.emailConfig.recipientEmail) document.getElementById('cfg-recipient').value = data.emailConfig.recipientEmail;
                    configLoaded = true;
                }

                // DynamoDB Rows
                const dynamoTbody = document.getElementById('dynamo-tbody');
                if (data.dynamoItems.length === 0) {
                    dynamoTbody.innerHTML = '<tr><td colspan="4" class="empty-state">No metadata recorded yet.</td></tr>';
                } else {
                    dynamoTbody.innerHTML = data.dynamoItems.map(item => \`
                        <tr>
                            <td style="font-family: monospace; color: #38bdf8;">\${item.ImageID.substring(0, 8)}...</td>
                            <td>\${item.FileName}</td>
                            <td>\${(item.FileSizeInBytes / 1024).toFixed(1)} KB</td>
                            <td style="color: var(--text-muted);">\${new Date(item.ProcessedAt).toLocaleTimeString()}</td>
                        </tr>
                    \`).join('');
                }

                // SNS Emails
                const snsContainer = document.getElementById('sns-container');
                if (data.snsNotifications.length === 0) {
                    snsContainer.innerHTML = '<div class="empty-state">No emails triggered yet.</div>';
                } else {
                    snsContainer.innerHTML = data.snsNotifications.map(email => \`
                        <div class="email-card">
                            <div class="email-subject">✉️ \${email.subject}</div>
                            <div class="email-body">\${email.message}</div>
                            <div class="email-time">Topic: MeraAlertSystem | Time: \${new Date(email.sentAt).toLocaleTimeString()}</div>
                        </div>
                    \`).join('');
                }

                // S3 Files
                const s3Tbody = document.getElementById('s3-tbody');
                if (data.s3Files.length === 0) {
                    s3Tbody.innerHTML = '<tr><td colspan="4" class="empty-state">Bucket is empty.</td></tr>';
                } else {
                    s3Tbody.innerHTML = data.s3Files.map(file => \`
                        <tr>
                            <td><img class="img-thumb" src="/s3-files/\${encodeURIComponent(file.name)}" alt="thumb" onerror="this.style.display='none'"></td>
                            <td style="font-family: monospace; color: #a5f3fc;">\${file.name}</td>
                            <td>\${(file.size / 1024).toFixed(1)} KB</td>
                            <td style="color: var(--text-muted);">\${new Date(file.createdAt).toLocaleString()}</td>
                        </tr>
                    \`).join('');
                }

            } catch (err) {
                console.error("Dashboard fetch error:", err);
            }
        }

        async function saveEmailConfig() {
            const senderEmail = document.getElementById('cfg-sender').value.trim();
            const senderAppPassword = document.getElementById('cfg-pass').value.trim();
            const recipientEmail = document.getElementById('cfg-recipient').value.trim();
            const msgEl = document.getElementById('email-action-msg');

            if (!senderEmail || !recipientEmail) {
                msgEl.innerHTML = '<span style="color: #ef4444;">❌ Kripya Sender aur Recipient email dono bharein!</span>';
                return;
            }

            msgEl.innerHTML = '<span style="color: #38bdf8;">Saving...</span>';
            const res = await fetch('/api/save-email-config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ senderEmail, senderAppPassword, recipientEmail })
            });
            const data = await res.json();
            if (res.ok) {
                msgEl.innerHTML = '<span style="color: #10b981;">✅ Settings Save ho gayi! Ab aap "Send Test" dabakar check kar sakte hain.</span>';
                fetchStatus();
            } else {
                msgEl.innerHTML = '<span style="color: #ef4444;">❌ ' + data.error + '</span>';
            }
        }

        async function sendTestEmail() {
            const msgEl = document.getElementById('email-action-msg');
            msgEl.innerHTML = '<span style="color: #38bdf8;">⏳ Test email bheji ja rahi hai...</span>';
            try {
                const res = await fetch('/api/send-test-email', { method: 'POST' });
                const data = await res.json();
                if (res.ok && data.success) {
                    msgEl.innerHTML = '<span style="color: #10b981;">🎉 Mubarak ho! Test Email aapke inbox me bhej di gayi hai! Kripya apna Inbox/Spam check karein.</span>';
                } else {
                    msgEl.innerHTML = '<span style="color: #ef4444;">❌ Email send nahi hui: ' + (data.error || data.reason || 'Check credentials') + '</span>';
                }
            } catch (e) {
                msgEl.innerHTML = '<span style="color: #ef4444;">❌ Error: ' + e.message + '</span>';
            }
        }

        setInterval(fetchStatus, 2000);
        fetchStatus();
    </script>
</body>
</html>`;
}

server.listen(PORT, () => {
    console.log(`\n======================================================`);
    console.log(`🚀 AWS Serverless Pipeline Simulator is ACTIVE!`);
    console.log(`======================================================`);
    console.log(`👉 Photo Uploader UI : http://localhost:${PORT}`);
    console.log(`👉 Live Cloud Monitor: http://localhost:${PORT}/dashboard`);
    console.log(`======================================================\n`);
});
