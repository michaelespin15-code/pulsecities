# v2.0 Deployment Reference: Copy-Paste Configuration Files

**Project:** PulseCities v2.0 Production Deployment
**Date:** 2026-04-16

This document provides exact configuration files to copy onto the DigitalOcean VPS. These are ready-to-deploy; only customize domain names and paths.

---

## 1. Gunicorn Systemd Service File

**File:** `/etc/systemd/system/pulsecities.service`

```ini
[Unit]
Description=PulseCities FastAPI Application
After=network.target postgresql.service

[Service]
Type=notify
User=www-data
Group=www-data
WorkingDirectory=/var/www/pulsecities
Environment="PATH=/var/www/pulsecities/venv/bin"
Environment="DATABASE_URL=postgresql://pulsecities_user:PASSWORD@localhost/pulsecities"
ExecStart=/var/www/pulsecities/venv/bin/gunicorn \
  -w 1 \
  -k uvicorn.workers.UvicornWorker \
  --bind unix:/var/www/pulsecities/pulsecities.sock \
  --timeout 120 \
  --access-logfile /var/log/pulsecities/access.log \
  --error-logfile /var/log/pulsecities/error.log \
  api.main:app
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**Installation:**
```bash
# Copy content above into:
sudo vim /etc/systemd/system/pulsecities.service

# Set permissions
sudo chmod 644 /etc/systemd/system/pulsecities.service

# Create log directory
sudo mkdir -p /var/log/pulsecities
sudo chown www-data:www-data /var/log/pulsecities

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable pulsecities
sudo systemctl start pulsecities

# Verify
sudo systemctl status pulsecities
sudo journalctl -u pulsecities -f  # Watch logs
```

**Critical customizations:**
- Change `PASSWORD` in DATABASE_URL to actual PostgreSQL password
- Verify working directory `/var/www/pulsecities` matches actual application path
- `-w 1`: Do NOT change this (single worker required for APScheduler)

---

## 2. Nginx Configuration

**File:** `/etc/nginx/sites-available/pulsecities`

```nginx
# Redirect HTTP to HTTPS
server {
    listen 80;
    server_name pulsecities.com www.pulsecities.com;
    return 301 https://$server_name$request_uri;
}

# HTTPS server
server {
    listen 443 ssl http2;
    server_name pulsecities.com www.pulsecities.com;
    client_max_body_size 10M;

    # SSL certificates (auto-configured by certbot)
    ssl_certificate /etc/letsencrypt/live/pulsecities.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/pulsecities.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;

    # Upstream to Gunicorn Unix socket
    upstream pulsecities {
        server unix:/var/www/pulsecities/pulsecities.sock fail_timeout=0;
    }

    # Root location: proxy to FastAPI
    location / {
        proxy_pass http://pulsecities;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_buffering off;
        proxy_request_buffering off;
    }

    # Static files (frontend HTML, CSS, JS)
    location /frontend/ {
        alias /var/www/pulsecities/frontend/;
        expires 1h;
        add_header Cache-Control "public, immutable";
    }

    # API endpoints (optional: separate config for different timeouts)
    location /api/ {
        proxy_pass http://pulsecities;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_buffering off;
        # Longer timeout for large operator portfolio queries
        proxy_read_timeout 60s;
    }

    # Health check endpoint (no proxying needed if using Cloudflare)
    location /health {
        proxy_pass http://pulsecities;
        access_log off;
    }
}
```

**Installation:**
```bash
# Copy content above into:
sudo vim /etc/nginx/sites-available/pulsecities

# Enable the site
sudo ln -s /etc/nginx/sites-available/pulsecities /etc/nginx/sites-enabled/pulsecities

# Disable default site if present
sudo unlink /etc/nginx/sites-enabled/default 2>/dev/null || true

# Test syntax
sudo nginx -t

# Reload
sudo systemctl reload nginx

# Verify
sudo systemctl status nginx
```

**Critical customizations:**
- Change `pulsecities.com` to actual domain name
- Verify Unix socket path `/var/www/pulsecities/pulsecities.sock` matches service file
- SSL certificate paths will be auto-updated by certbot (don't change manually)

---

## 3. Certbot SSL Setup

**Installation and certificate request:**

```bash
# Install Certbot
sudo apt update
sudo apt install -y certbot python3-certbot-nginx

# Request certificate and auto-configure Nginx
# (Interactive; prompts for email, domain confirmation)
sudo certbot --nginx -d pulsecities.com -d www.pulsecities.com

# Certbot automatically:
# 1. Obtains certificate from Let's Encrypt
# 2. Updates /etc/nginx/sites-available/pulsecities with SSL config
# 3. Reloads Nginx
# 4. Installs systemd timer for auto-renewal

# Verify auto-renewal timer
sudo systemctl status certbot.timer
sudo systemctl enable certbot.timer
sudo systemctl start certbot.timer

# Test renewal (dry-run, doesn't actually renew)
sudo certbot renew --dry-run

# Monitor renewal logs
sudo journalctl -u certbot.timer -f
```

**Certificate renewal:** Runs automatically every 12 hours. No manual intervention needed.

**Troubleshooting:** If renewal fails repeatedly:
```bash
# Check status
sudo certbot renew --verbose

# Manual renewal
sudo certbot renew --force-renewal

# Check email for renewal notifications
```

---

## 4. Deployment Walkthrough

### Step 1: Pre-Deployment Verification (Local Development)

```bash
# Verify Gunicorn starts locally
cd /path/to/pulsecities
gunicorn -w 1 -k uvicorn.workers.UvicornWorker api.main:app

# Curl localhost:8000 to verify it's serving
curl http://localhost:8000/api/health

# Ctrl+C to stop

# Check that APScheduler would run only once (look for single "APScheduler started" log)
```

### Step 2: Provision VPS

```bash
# SSH to VPS
ssh -i /path/to/key root@104.236.87.19

# Update system
sudo apt update && sudo apt upgrade -y

# Install dependencies
sudo apt install -y python3.11 python3.11-venv postgresql-client nginx certbot

# Create application user
sudo useradd -m -s /bin/bash www-data 2>/dev/null || true
```

### Step 3: Deploy Application

```bash
# Clone repo
sudo mkdir -p /var/www
sudo git clone https://github.com/your-org/pulsecities.git /var/www/pulsecities

# Create Python venv
sudo python3.11 -m venv /var/www/pulsecities/venv

# Install dependencies
sudo /var/www/pulsecities/venv/bin/pip install -r /var/www/pulsecities/requirements.txt

# Fix permissions
sudo chown -R www-data:www-data /var/www/pulsecities

# Create directories
sudo mkdir -p /var/log/pulsecities
sudo chown www-data:www-data /var/log/pulsecities
```

### Step 4: Configure Gunicorn & Systemd

```bash
# Copy service file (see section 1 above)
sudo vim /etc/systemd/system/pulsecities.service
# (Paste content, customize DATABASE_URL, save)

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable pulsecities
sudo systemctl start pulsecities

# Verify
sudo systemctl status pulsecities
```

### Step 5: Configure Nginx

```bash
# Copy Nginx config (see section 2 above)
sudo vim /etc/nginx/sites-available/pulsecities
# (Paste content, customize domain, save)

# Enable
sudo ln -s /etc/nginx/sites-available/pulsecities /etc/nginx/sites-enabled/pulsecities
sudo unlink /etc/nginx/sites-enabled/default 2>/dev/null || true

# Test
sudo nginx -t

# Reload
sudo systemctl reload nginx
```

### Step 6: Obtain SSL Certificate

```bash
# Certbot interactive setup
# (Answers: email, agree to terms, don't require HTTPS yet, etc.)
sudo certbot --nginx -d pulsecities.com -d www.pulsecities.com

# Certbot modifies /etc/nginx/sites-available/pulsecities automatically
# Verify it looks correct
sudo cat /etc/nginx/sites-available/pulsecities | grep ssl

# Enable auto-renewal
sudo systemctl enable certbot.timer
sudo systemctl start certbot.timer
```

### Step 7: Verify Full Stack

```bash
# Test HTTP → HTTPS redirect
curl -i http://pulsecities.com  # Should 301 redirect to HTTPS

# Test HTTPS endpoint
curl -k https://pulsecities.com/api/health

# Check Gunicorn logs
sudo journalctl -u pulsecities -f

# Check Nginx logs
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

### Step 8: Monitor & Maintain

```bash
# Daily check
sudo systemctl status pulsecities
sudo systemctl status nginx
sudo systemctl status certbot.timer

# Weekly check (SSL renewal)
sudo certbot renew --dry-run

# Disk usage
df -h
du -sh /var/www/pulsecities
du -sh /var/log/pulsecities

# Process count (should be 1 Gunicorn + 1 Uvicorn)
ps aux | grep -E 'gunicorn|uvicorn'
```

---

## 5. Troubleshooting Quick Reference

### Gunicorn Not Starting

```bash
# Check logs
sudo journalctl -u pulsecities -n 20

# Verify service file syntax
sudo systemctl status pulsecities -l

# Test manually
sudo -u www-data /var/www/pulsecities/venv/bin/gunicorn \
  -w 1 -k uvicorn.workers.UvicornWorker \
  --bind unix:/var/www/pulsecities/pulsecities.sock \
  api.main:app

# Common issues:
# - Wrong DATABASE_URL: check systemd Environment= line
# - Wrong working directory: check WorkingDirectory=
# - Socket permission: verify /var/www/pulsecities ownership is www-data:www-data
```

### Nginx 502 Bad Gateway

```bash
# Gunicorn is down or socket doesn't exist
ls -la /var/www/pulsecities/pulsecities.sock

# Check permissions
sudo ls -l /var/www/pulsecities/
# www-data must own socket

# Restart Gunicorn
sudo systemctl restart pulsecities

# Check Nginx error log
sudo tail -f /var/log/nginx/error.log
```

### SSL Certificate Not Renewing

```bash
# Check certbot timer
sudo systemctl status certbot.timer

# Force renewal
sudo certbot renew --force-renewal

# Check logs
sudo journalctl -u certbot.timer -f

# Certificate expiration
sudo certbot certificates
```

### APScheduler Running Multiple Times

```bash
# Check logs for duplicate job runs
sudo journalctl -u pulsecities | grep APScheduler

# If duplicates, verify Gunicorn has -w 1
ps aux | grep gunicorn
# Should show ONE gunicorn process, not multiple

# If multiple, restart with correct config
sudo systemctl restart pulsecities
```

---

## 6. Monitoring Commands

```bash
# Real-time process monitoring
watch -n 2 'ps aux | grep -E "gunicorn|uvicorn" | grep -v grep'

# Real-time Gunicorn logs
sudo journalctl -u pulsecities -f

# Real-time Nginx logs
sudo tail -f /var/log/nginx/access.log

# SSL certificate status
sudo certbot certificates

# Disk usage
du -sh /var/www/pulsecities /var/log/pulsecities

# System resources
free -h
df -h
uptime

# PostgreSQL connections
sudo -u postgres psql -c "SELECT count(*) as connection_count FROM pg_stat_activity WHERE datname = 'pulsecities';"
```

---

## 7. Rollback Plan

If something goes wrong during deployment:

```bash
# Revert Nginx config
sudo unlink /etc/nginx/sites-enabled/pulsecities
sudo ln -s /etc/nginx/sites-available/default /etc/nginx/sites-enabled/default
sudo systemctl reload nginx

# Stop Gunicorn
sudo systemctl stop pulsecities

# Revert code (if applicable)
cd /var/www/pulsecities && git reset --hard HEAD~1

# Restart
sudo systemctl start pulsecities
```

---

## 8. Performance Baseline (for comparison)

After deployment, establish baseline metrics:

```bash
# Request latency (API endpoint)
curl -w "\nTotal time: %{time_total}s\n" https://pulsecities.com/api/health

# Static asset load time (Nginx serving HTML)
curl -w "\nTotal time: %{time_total}s\n" https://pulsecities.com/frontend/index.html

# Concurrent connections test (100 requests)
ab -n 100 -c 10 https://pulsecities.com/api/health

# Check system load
watch -n 1 'top -bn1 | head -10'
```

Expected on 2 vCPU / 2GB RAM:
- API latency: 50–200ms (mostly database query time)
- Static assets: <10ms
- Can handle ~100 concurrent connections comfortably
- CPU usage: <20% during normal traffic

---

## 9. Emergency Contacts & Next Steps

After deployment is live:

1. **Monitor for 48 hours**: Check logs, verify APScheduler runs once nightly
2. **Update DNS records** (if not done already): Point pulsecities.com to 104.236.87.19
3. **Test from external IP**: Verify SSL certificate is valid from browser
4. **Set up monitoring**: Optional: Datadog, New Relic, or simple cron email alerts
5. **Document any customizations** made during deployment

---

*Deployment Reference for PulseCities v2.0*
*Created: 2026-04-16*
*Ready for production use*
