# deploy/deploy.ps1
# Full deploy: push to OCI + deploy frontend to Cloudflare Pages (mem-backend2)
# Usage: .\deploy\deploy.ps1

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path $PSScriptRoot -Parent
$SshKey   = "$env:USERPROFILE\Downloads\ssh-key-2026-02-27.key"
$Server   = "ubuntu@132.145.33.75"

Write-Host "[1/3] Pushing to GitHub..." -ForegroundColor Cyan
git -C $RepoRoot push

Write-Host "[2/3] Deploying backend to OCI..." -ForegroundColor Cyan
ssh -i $SshKey $Server "cd /home/ubuntu/trading-galaxy && git pull origin master && sudo systemctl restart trading-galaxy && sleep 2 && sudo systemctl is-active trading-galaxy"

Write-Host "[3/3] Deploying frontend to Cloudflare Pages (mem-backend2)..." -ForegroundColor Cyan
Set-Location $RepoRoot
npx wrangler pages deploy static --project-name mem-backend2 --branch master --commit-dirty=true

Write-Host "Done." -ForegroundColor Green
