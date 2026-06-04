# Backup script for mrr, stg, dwh databases
# Run: bash backup.sh

# Setting

$PG_CONTAINER = "postgres_dwh"
$PG_USER      = "dwh_user"
$BACKUP_DIR   = ".\backups"
$TIMESTAMP    = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$DATABASES    = @("mrr", "stg", "dwh")

# Creating backup folder if it doesn't exist
New-Item -ItemType Directory -Force -Path $BACKUP_DIR | Out-Null

Write-Host "Backup started: $TIMESTAMP"

# Iterating over all databases
foreach ($DB in $DATABASES) {

    $BACKUP_FILE = "$BACKUP_DIR\${DB}_${TIMESTAMP}.sql"

    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Starting backup: $DB → $BACKUP_FILE"

    # pg_dump inside the container -> save file to host
    docker exec $PG_CONTAINER pg_dump -U $PG_USER -d $DB | Out-File -FilePath $BACKUP_FILE -Encoding utf8

    # Checking success
    if ($LASTEXITCODE -eq 0) {
        $SIZE = (Get-Item $BACKUP_FILE).Length / 1KB
        $SIZE = [math]::Round($SIZE, 1)
        Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $DB backup success — size: ${SIZE}KB"
    } else {
        Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $DB backup failed"
    }
}

Write-Host "Backup finished: $(Get-Date -Format 'HH:mm:ss')"
Write-Host "Backup location: $BACKUP_DIR"
