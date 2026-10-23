# Corrido por el Programador de Tareas de Windows a las 11:00 y a las 20:00 todos los
# dias. Si el catalogo YA se SINCRONIZO bien hoy (scrape + ingest), la de las 20:00 no
# vuelve a correr - evita duplicar el scrape de 20-25 min dos veces el mismo dia si la
# primera anduvo bien.
#
# Senal de "sincronizo bien hoy": el archivo marcador ultima-sync-ok.txt, que se
# (re)escribe SOLO cuando correr-automatico.bat termina con exit code 0.
#
# OJO - por que NO alcanza con mirar remax_cordoba_properties.json (como antes):
# scrape.py reescribe ese json al terminar de scrapear, ANTES de postear a
# market-ingest. Si el scrape anda pero el ingest falla (HTTP 500, sin internet,
# etc.), el json igual queda con la fecha de hoy y "mentiria" que todo salio bien,
# bloqueando el reintento de las 20:00. El exit code de scrape.py si refleja el
# ingest: post_to_ingest hace raise_for_status(), que corta el script con codigo
# != 0 si alguna tanda fallo. (Incidente 2026-10-23: la tanda 10 dio HTTP 500 por
# un source_id duplicado en el lote; el json quedo fresco y el viejo check marcaba
# "Scrape terminado OK" aunque el catalogo NO se habia sincronizado.)
#
# NOTA: este archivo tiene que quedar en ASCII puro (sin tildes ni rayas largas) -
# PowerShell 5.1 rompe el parseo de scripts .ps1 guardados en UTF-8 sin BOM si tienen
# caracteres no-ASCII (confirmado en vivo 2026-09-13, "Falta la cadena en el
# terminador" apuntando justo a un caracter con tilde).

$dir = "C:\Users\marce\remax-catalogo-scraper"
Set-Location $dir
$logFile = Join-Path $dir "task-log.txt"
$okMarker = Join-Path $dir "ultima-sync-ok.txt"

function Log($msg) {
  $linea = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') - $msg"
  Add-Content -Path $logFile -Value $linea -Encoding UTF8
}

$yaCorrioHoy = (Test-Path $okMarker) -and ((Get-Item $okMarker).LastWriteTime.Date -eq (Get-Date).Date)

if ($yaCorrioHoy) {
  Log "Catalogo ya sincronizado hoy (scrape + ingest OK) - no hace falta correr de nuevo."
  exit 0
}

Log "Catalogo desactualizado - arrancando scrape..."
& cmd.exe /c "`"$dir\correr-automatico.bat`""
$code = $LASTEXITCODE

if ($code -eq 0) {
  Set-Content -Path $okMarker -Value (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') -Encoding ASCII
  Log "Scrape + ingest OK (exit 0)."
} else {
  Log "FALLO: scrape/ingest termino con exit $code - el catalogo NO se sincronizo. Revisar salida.txt. Reintenta a las 20:00 si esta corrida fue la de las 11:00."
}
