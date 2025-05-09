# Multi-Instance Go App Deployment README

This guide outlines how to deploy and run multiple instances of the `sherpa` Go app with unique environments, systemd-managed services, and reverse-proxied URLs on a single EC2 server.

---

## ⚙️ Step 0: EC2 & Security Group Setup

1. **Launch EC2 Instance**:
   - Ubuntu 20.04 or 22.04
   - T3 or T4 instance types recommended

2. **Configure Security Group**:
   - Use existing template: `sg-06387742a27a5a68c (launch-wizard-50)`
   - Allow:
     - TCP 22 (SSH)
     - TCP 80 (HTTP)
     - TCP 4000–4010 (for internal port access)

---

## 🔑 Step 1: SSH & Tooling Setup

1. **SSH into instance**:
   ```bash
   ssh -i your-key.pem ubuntu@<EC2-IP>
   ```

2. **Install required packages**:
   ```bash
   sudo apt update
   sudo apt install -y golang git nginx ufw tree
   ```

3. **Generate SSH key (if not done already)**:
   ```bash
   ssh-keygen -t rsa -b 4096 -C "hem.malhotra@swayy.com.au"
   cat ~/.ssh/id_rsa.pub
   ```

4. **Add your public key to GitHub**:
   - Go to [GitHub SSH Keys](https://github.com/settings/keys)
   - Add the copied key as a new "SSH Key"

5. **Test GitHub SSH access**:
   ```bash
   ssh -T git@github.com
   ```

---

## 🧬 Step 2: Clone and Prepare Repo

1. **Clone the repo using SSH**:
   ```bash
   git clone git@github.com:mnm458/sherpa.git
   cd sherpa
   ```

2. **Checkout the correct branch**:
   ```bash
   git checkout feat/websocket-refactor
   ```
   
3. **Init the Git dependencies**:
   ```bash
   git submodule init
   ```
   
3. **Update the Git dependencies**:
   ```bash
   git submodule update
   ```
   
3. **Tidy dependencies**:
   ```bash
   go mod tidy
   ```

---

## 🛠️ Step 3: Build Go Executable

From within the `sherpa` directory:
```bash
go build -o sherpa ./cmd/web
```

---

## 🗂️ Step 4: Prepare Instances

1. **Create instance directories**:
   ```bash
   mkdir -p ~/instances/gs{1..6}
   ```

2. **Copy binary into each instance**:
   ```bash
   for i in {1..6}; do
     cp sherpa ~/instances/gs$i/
     chmod +x ~/instances/gs$i/sherpa
   done
   ```

3. **Add a unique `.env` file to each instance**:
   ```bash
   cp .env.example ~/instances/gs1/.env
   # Modify manually per instance or template with sed
   ```

---

## 🧩 Step 5: Create systemd Service Template

1. **Create the service file**:
   ```bash
   sudo vi /etc/systemd/system/sherpa@.service
   ```

2. **Paste the following content**: (for BINANCE set `-exchange binance` and for BYBIT set `-exchange bybit`
   ```ini
   [Unit]
   Description=Sherpa Instance %i
   After=network.target

   [Service]
   WorkingDirectory=/home/ubuntu/instances/gs%i
   ExecStart=/home/ubuntu/instances/gs%i/sherpa -exchange binance -env prod -addr :400%i -reEntrySwitch false
   EnvironmentFile=/home/ubuntu/instances/gs%i/.env
   Restart=always
   User=ubuntu

   StandardOutput=append:/home/ubuntu/instances/gs%i/gs%i.txt
   StandardError=append:/home/ubuntu/instances/gs%i/gs%i.txt

   [Install]
   WantedBy=multi-user.target
   ```

3. **Reload systemd and start instances**:
   ```bash
   sudo systemctl daemon-reload
   for i in {1..6}; do
     sudo systemctl enable sherpa@$i
     sudo systemctl start sherpa@$i
   done
   ```

---

## 🌐 Step 6: Configure Nginx Reverse Proxy

1. **Edit default site**:
   ```bash
   sudo vi /etc/nginx/sites-available/default
   ```

2. **Paste the following**: (use `:1,$d` to clear file contents first)
   ```nginx
   server {
       listen 80;
       server_name _;

       location /gs1/ {
           proxy_pass http://localhost:4001/;
           proxy_set_header Host $host;
           rewrite ^/gs1/(.*)$ /$1 break;
       }

       location /gs2/ {
           proxy_pass http://localhost:4002/;
           proxy_set_header Host $host;
           rewrite ^/gs2/(.*)$ /$1 break;
       }

       location /gs3/ {
           proxy_pass http://localhost:4003/;
           proxy_set_header Host $host;
           rewrite ^/gs3/(.*)$ /$1 break;
       }

       location /gs4/ {
           proxy_pass http://localhost:4004/;
           proxy_set_header Host $host;
           rewrite ^/gs4/(.*)$ /$1 break;
       }

       location /gs5/ {
           proxy_pass http://localhost:4005/;
           proxy_set_header Host $host;
           rewrite ^/gs5/(.*)$ /$1 break;
       }

       location /gs6/ {
           proxy_pass http://localhost:4006/;
           proxy_set_header Host $host;
           rewrite ^/gs6/(.*)$ /$1 break;
       }
   }
   ```

3. **Restart Nginx**:
   ```bash
   sudo nginx -t
   sudo systemctl restart nginx
   ```

4. **Allow HTTP traffic via firewall**:
   ```bash
   sudo ufw allow 'Nginx Full'
   sudo ufw reload
   ```

---

## 🔄 Step 7: Log Management & Cleanup

1. **Tail logs**:
   ```bash
   tail -f /home/ubuntu/instances/gs1/gs1.txt
   ```

2. **Delete logs if needed**:
   ```bash
   rm /home/ubuntu/instances/gs*/gs*.txt
   ```

---

## 🔁 Step 8: Reloading After `.env` or Account Changes

### 🔃 Restart a single instance after env update:
```bash
sudo systemctl restart sherpa@3
```

### 🔃 Restart all instances:
```bash
for i in {1..6}; do
  sudo systemctl restart sherpa@$i
done
```

### 🔍 Check status:
```bash
sudo systemctl status sherpa@4
```

---

## ✅ Endpoints Summary

| Instance | Port  | Public URL                 |
|----------|-------|----------------------------|
| gs1      | 4001  | http://{EC2-PUBLIC-IP}/gs1/       |
| gs2      | 4002  | http://{EC2-PUBLIC-IP}/gs2/       |
| gs3      | 4003  | http://{EC2-PUBLIC-IP}/gs3/       |
| gs4      | 4004  | http://{EC2-PUBLIC-IP}/gs4/       |
| gs5      | 4005  | http://{EC2-PUBLIC-IP}/gs5/       |
| gs6      | 4006  | http://{EC2-PUBLIC-IP}/gs6/       |

---
