# Git Repository Setup Script for Windows PowerShell
# Run this script to initialize your local repository

$ProjectPath = "c:\Users\rachit.jain\Desktop\Python projects\Payments"
Set-Location $ProjectPath

Write-Host "================================================" -ForegroundColor Green
Write-Host "Git Repository Setup for RBI Data Sync" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host ""

# Check if Git is installed
try {
    $GitVersion = git --version
    Write-Host "✓ Git is installed: $GitVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ Git is not installed. Please install Git from https://git-scm.com/download/win" -ForegroundColor Red
    exit 1
}

# Configure Git (if not already configured)
Write-Host ""
Write-Host "Configuring Git user..." -ForegroundColor Yellow

# Check if git user is already configured
$GitUserName = git config --global user.name
if (-not $GitUserName) {
    Write-Host "Git user name not configured. Enter your name:"
    $UserName = Read-Host "Name"
    git config --global user.name $UserName
    Write-Host "✓ Git user name set to: $UserName" -ForegroundColor Green
} else {
    Write-Host "✓ Git user name already configured: $GitUserName" -ForegroundColor Green
}

$GitUserEmail = git config --global user.email
if (-not $GitUserEmail) {
    Write-Host "Git user email not configured. Enter your email:"
    $UserEmail = Read-Host "Email"
    git config --global user.email $UserEmail
    Write-Host "✓ Git user email set to: $UserEmail" -ForegroundColor Green
} else {
    Write-Host "✓ Git user email already configured: $GitUserEmail" -ForegroundColor Green
}

# Initialize local repository
Write-Host ""
Write-Host "Initializing local Git repository..." -ForegroundColor Yellow

if (Test-Path ".git") {
    Write-Host "✓ Repository already initialized" -ForegroundColor Green
} else {
    git init
    Write-Host "✓ Initialized local repository" -ForegroundColor Green
}

# Add all files
Write-Host ""
Write-Host "Adding files to staging area..." -ForegroundColor Yellow
git add .
$FilesAdded = (git status --short | Measure-Object).Count
Write-Host "✓ Added $FilesAdded files" -ForegroundColor Green

# Create initial commit
Write-Host ""
Write-Host "Creating initial commit..." -ForegroundColor Yellow
git commit -m "Initial commit: RBI data sync automation setup"
Write-Host "✓ Initial commit created" -ForegroundColor Green

# Show status
Write-Host ""
Write-Host "Current Git status:" -ForegroundColor Yellow
git status

Write-Host ""
Write-Host "================================================" -ForegroundColor Green
Write-Host "✓ Local Git repository setup complete!" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Create a new repository on GitHub (https://github.com/new)" -ForegroundColor Cyan
Write-Host "2. Run AFTER creating the GitHub repo:" -ForegroundColor Cyan
Write-Host "   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git" -ForegroundColor Cyan
Write-Host "3. Push to GitHub:" -ForegroundColor Cyan
Write-Host "   git branch -M main" -ForegroundColor Cyan
Write-Host "   git push -u origin main" -ForegroundColor Cyan
Write-Host ""
