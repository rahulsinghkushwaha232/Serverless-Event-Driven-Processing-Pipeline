# ☁️ Serverless Event-Driven Processing Pipeline

An event-driven cloud pipeline architecture utilizing **Amazon S3, AWS Lambda, Amazon DynamoDB, and Amazon SNS** with an interactive **Local Cloud Simulator & Real-Time Monitor**.

---

## 🏗️ Architecture Overview

```text
[ Frontend Uploader ]
         │
         ▼
[ API Gateway / Upload Endpoint ]
         │
         ▼
[ Amazon S3 Bucket ] ────── (ObjectCreated Event) ──────┐
                                                       ▼
                                             [ AWS Lambda Trigger ]
                                                       │
                           ┌───────────────────────────┴───────────────────────────┐
                           ▼                                                       ▼
               [ Amazon DynamoDB ]                                     [ Amazon SNS Topic ]
             (ImageMetadataTable)                                    (Real Email Notification)
```

1. **Frontend Uploader**: Simple, clean UI for selecting and converting images into Base64 payload.
2. **API Gateway**: REST endpoint (`POST /upload`) accepting image payloads.
3. **Amazon S3**: Secure object storage triggering asynchronous `ObjectCreated` event notifications.
4. **AWS Lambda**: Serverless compute extracting image metadata (UUID, filename, file size, timestamps).
5. **Amazon DynamoDB**: NoSQL database table (`ImageMetadataTable`) storing structured image records.
6. **Amazon SNS**: Notification topic (`MeraAlertSystem`) delivering real-time email alerts directly to your inbox.
7. **Live Monitor Dashboard**: Real-time visual architecture dashboard displaying S3 files, Lambda logs, DynamoDB records, and SNS alerts.

---

## 🚀 Getting Started (Local Runner)

You can run and test this entire event-driven architecture locally without incurring any AWS charges or requiring cloud credits:

### 1. Prerequisites
- [Node.js](https://nodejs.org/) (v16 or higher)

### 2. Quick Start
Just double-click **`start.bat`** (on Windows) or run:
```bash
node server.js
```

### 3. Open in Browser
- **Photo Uploader**: [http://localhost:3000](http://localhost:3000)
- **Live Architecture Monitor**: [http://localhost:3000/dashboard](http://localhost:3000/dashboard)

---

## 📬 Real Email Notification Setup

To receive real email alerts upon file upload:
1. Copy `email_config.example.json` to `email_config.json`:
   ```bash
   cp email_config.example.json email_config.json
   ```
2. Add your Gmail and 16-character [Google App Password](https://myaccount.google.com/apppasswords):
   ```json
   {
     "enabled": true,
     "service": "gmail",
     "senderEmail": "your-email@gmail.com",
     "senderAppPassword": "xxxx xxxx xxxx xxxx",
     "recipientEmail": "your-email@gmail.com"
   }
   ```
   *(Or configure it directly via the web dashboard at `http://localhost:3000/dashboard`)*

---

## 📁 Project Structure

```
├── backend/
│   └── lambda_function.py           # AWS Lambda Python function handler
├── frontend/
│   └── index.html                   # Photo Uploader UI
├── server.js                        # Local Cloud Pipeline Simulator & API
├── start.bat                        # 1-Click launcher script (Windows)
├── docker-compose.yml               # LocalStack Docker configuration
├── email_config.example.json        # Template for email credentials
├── Screenshort image/               # Project phase screenshots
└── README.md                        # Documentation
```

---

## 🔒 Security Note
Private credentials and local uploads are automatically excluded via `.gitignore` to ensure secure version control.
