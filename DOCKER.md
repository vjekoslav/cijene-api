# Docker dokumentacija

Ovaj dokument pruža sveobuhvatne informacije o Docker postavkama za Cijene API projekt.

## Pregled

Projekt koristi Docker za kontejnerizaciju servisa za praćenje cijena hrvatskih trgovačkih lanaca, koji se sastoji od:
- **API servis**: FastAPI web servis za pružanje podataka o cijenama
- **Crawler servis**: Preuzimanje podataka o cijenama s web stranica trgovačkih lanaca
- **Baza podataka**: PostgreSQL za pohranu podataka
- **Adminer**: Sučelje za administraciju baze podataka (samo za razvoj)

## Arhitektura

### Višefazni Dockerfile

Projekt koristi sofisticirani višefazni Dockerfile s tri faze:

#### 1. Osnovna faza (`base`)
- **Osnovna slika**: `python:3.13-slim`
- **Svrha**: Zajednička osnova sa sistemskim ovisnostima
- **Značajke**:
  - Optimizirane Python environment varijable
  - Sistemski paketi (build alati, curl, podrška za vremensku zonu)
  - Zakucana uv verzija (0.7.14) za reproducibilnost
  - Europe/Zagreb konfiguracija vremenske zone
  - Metadata labeli kontejnera

#### 2. Faza ovisnosti (`deps`)
- **Proširuje**: Osnovnu fazu
- **Svrha**: Instalacija Python ovisnosti
- **Značajke**:
  - Samo produkcijske ovisnosti (`uv sync --frozen --no-dev`)
  - Optimizirano za cache slojeva
  - Kopira samo `pyproject.toml` i `uv.lock`

#### 3. Razvojna faza (`development`)
- **Proširuje**: Fazu ovisnosti
- **Svrha**: Razvojno okruženje sa svim ovisnostima
- **Značajke**:
  - Sve ovisnosti uključujući dev alate
  - Ne-root korisnik (`appuser`, UID 1001)
  - Podrška za mount izvornog koda
  - Mogućnost hot reload-a

#### 4. Produkcijska faza (`production`)
- **Proširuje**: Fazu ovisnosti
- **Svrha**: Optimizirano produkcijsko pokretanje
- **Značajke**:
  - Ne-root korisnik za sigurnost
  - Ugrađena zdravstvena provjera
  - Minimalna površina napada
  - Produkcijski optimizirane postavke

## Konfiguracija servisa

### Servis baze podataka (`db`)

```yaml
services:
  db:
    image: postgres:17-alpine
    container_name: cijene-db
```

**Značajke**:
- PostgreSQL 17 Alpine za minimalnu veličinu
- Automatska inicijalizacija sheme iz `service/db/psql.sql`
- Zdravstvene provjere s `pg_isready`
- Trajna pohrana podataka s imenovanim volumenom
- Konfiguracija na temelju environment varijabli

**Zdravstvena provjera**:
- Naredba: `pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}`
- Interval: 10 sekundi
- Timeout: 5 sekundi
- Početni period: 10 sekundi
- Ponovni pokušaji: 5

### API servis (`api`)

```yaml
services:
  api:
    build:
      context: .
      target: production
    container_name: cijene-api
```

**Značajke**:
- Izgrađen iz produkcijske faze Dockerfile-a
- Ovisi o zdravom servisu baze podataka
- Praćenje health endpoint-a
- Read-only mount podatkovnih volumena
- Automatski restart pri grešci

**Zdravstvena provjera**:
- Naredba: `curl -f http://localhost:8000/health`
- Interval: 30 sekundi
- Timeout: 10 sekundi
- Početni period: 30 sekundi
- Ponovni pokušaji: 3

### Crawler servis (`crawler`)

```yaml
services:
  crawler:
    build:
      context: .
      target: production
    profiles:
      - crawler
```

**Značajke**:
- Aktivacija na temelju profila (`crawler`)
- Jednokratno izvršavanje (`restart: "no"`)
- Read-write pristup podacima i output direktorijima
- Ručno ili cron-triggerirano izvršavanje

## Konfiguracija okruženja

### Environment datoteka (.env)

Projekt zahtijeva `.env` datoteku za konfiguraciju:

```bash
cp .env.example .env
```

**Ključne kategorije konfiguracije**:

#### Postavke aplikacije
- `VERSION`: Verzija aplikacije (0.1.0)
- `DEBUG`: Debug način rada (false za produkciju)
- `DEV_MODE`: Razvojne značajke (false za produkciju)
- `TIMEZONE`: Vremenska zona aplikacije (Europe/Zagreb)

#### Konfiguracija servera
- `HOST`: Bind adresa (0.0.0.0)
- `PORT`: Port servisa (8000)
- `BASE_URL`: Javni API URL
- `REDIRECT_URL`: Odredište root preusmjeravanja

#### Konfiguracija baze podataka
- `POSTGRES_DB`: Ime baze podataka (cijene)
- `POSTGRES_USER`: Korisnik baze podataka (cijene_user)
- `POSTGRES_PASSWORD`: Lozinka baze podataka
- `DB_DSN`: Potpuni connection string
- `DB_MIN_CONNECTIONS`: Minimalna veličina pool-a (5)
- `DB_MAX_CONNECTIONS`: Maksimalna veličina pool-a (20)

#### Pohrana podataka
- `ARCHIVE_DIR`: Putanja za pohranu arhiva (/app/data)
- `CRAWLER_OUTPUT_DIR`: Putanja crawler output-a (/app/output)

### Razvojni override-i

`docker-compose.override.yml` datoteka pruža konfiguraciju specifičnu za razvoj:

**Razvojne značajke**:
- Mount izvornog koda za hot reload
- Razvojna Docker faza s dev ovisnostima
- Omogućen debug način rada
- Adminer sučelje baze podataka
- Poboljšano logiranje i debugging

**Dodatni servisi**:
- **Adminer**: Administracija baze podataka na `http://localhost:8080`
  - Auto-konfiguriran s environment varijablama
  - Direktan pristup bazi podataka za razvoj

## Optimizacija buildanja

### Docker layer cache-iranje

Dockerfile je optimiziran za efikasno buildanje:

1. **Osnovno sistemsko postavljanje** (rijetko se mijenja)
2. **Instalacija ovisnosti** (mijenja se s ažuriranjem paketa)
3. **Kod aplikacije** (često se mijenja)

### .dockerignore konfiguracija

Optimizirani build kontekst isključuje:
- Python bytecode i cache datoteke
- Virtualna okruženja
- IDE konfiguraciju
- OS-specifične datoteke
- Output razvojnih alata
- Test datoteke
- Dokumentacijske datoteke
- Privremene datoteke

**Zadržano za buildanje**:
- Izvorni kod
- Konfiguracijske datoteke
- Podatkovni direktoriji (za razvoj)

## Obrasci korištenja

### Produkcijsko pokretanje

```bash
# Postavljanje okruženja
cp .env.example .env
# Uredite .env za produkciju

# Pokretanje osnovnih servisa
docker-compose up -d

# Provjera zdravlja servisa
docker-compose ps
docker-compose logs api
```

### Razvojni workflow

```bash
# Postavljanje okruženja
cp .env.example .env

# Pokretanje s razvojnim override-ima
docker-compose up -d

# Pristup servisima
# API: http://localhost:8000
# API dokumentacija: http://localhost:8000/docs
# Adminer: http://localhost:8080
```

### Crawler operacije

```bash
# Ručno izvršavanje crawler-a
docker-compose run --rm crawler

# Automatsko postavljanje crawler-a
./setup-cron.sh

# Ručna cron konfiguracija
sudo crontab -e
# Dodajte: 0 9 * * * cd /path/to/project && docker-compose run --rm crawler
```

### Upravljanje podacima

```bash
# Uvoz crawler podataka
docker-compose exec api uv run -m service.db.import /app/output/2024-01-15

# Obogaćivanje podataka o proizvodima
docker-compose exec api uv run -m service.db.enrich /app/enrichment/products.csv

# Pristup bazi podataka
docker-compose exec db psql -U cijene_user -d cijene
```

## Automatizirano crawlanje

### Host-based cron postavljanje

Projekt uključuje `setup-cron.sh` za automatsko raspoređivanje crawler-a:

**Značajke**:
- Automatska detekcija Docker Compose
- Podrška za `docker-compose` i `docker compose`
- Dvostruko dnevno izvršavanje (9 ujutro i 6 navečer Zagreb vrijeme)
- Sveobuhvatno logiranje
- Laka instalacija i uklanjanje

**Instalacija**:
```bash
./setup-cron.sh
```

**Verifikacija**:
```bash
# Provjera instaliranih cron job-ova
sudo crontab -l

# Prikaz crawler logova
tail -f /var/log/cijene-crawler.log
```

## Praćenje i zdravlje

### Zdravstvene provjere

Svi servisi uključuju sveobuhvatno praćenje zdravlja:

**Zdravlje baze podataka**:
- PostgreSQL ready provjera
- Validacija veze
- Brza detekcija pokretanja

**Zdravlje API-ja**:
- HTTP endpoint verifikacija
- Provjera dostupnosti servisa
- Validacija zdravlja ovisnosti

### Upravljanje kontejnerima

**Imenovani kontejneri**:
- `cijene-db`: Servis baze podataka
- `cijene-api`: API servis (produkcija)
- `cijene-api-dev`: API servis (razvoj)
- `cijene-crawler`: Crawler servis
- `cijene-adminer`: Sučelje za administraciju baze podataka

**Upravljanje volumenima**:
- `postgres_data`: Trajna pohrana baze podataka
- Host volumeni za podatke i konfiguraciju

## Sigurnosne značajke

### Ne-root izvršavanje

Svi aplikacijski kontejneri rade kao ne-root korisnik:
- **Korisnik**: `appuser`
- **UID**: 1001
- **Sigurnost**: Sprječava eskalaciju privilegija

### Read-only mount-ovi

Produkcijski volumeni mount-ani su read-only gdje je to prikladno:
- Podatkovni direktoriji: Read-only za API servis
- Konfiguracijske datoteke: Read-only mount-ovi

### Mrežna izolacija

Servisi komuniciraju kroz Docker-ovu zadanu bridge mrežu:
- Interna komunikacija servis-do-servis
- Nema nepotrebnog vanjskog izlaganja
- Konfigurirano port mapiranje

## Rješavanje problema

### Česti problemi

**Greške buildanja**:
```bash
# Clean build
docker-compose build --no-cache

# Provjera build konteksta
docker build . --progress=plain
```

**Problemi s okruženjem**:
```bash
# Validacija konfiguracije
docker-compose config

# Provjera učitavanja okruženja
docker-compose exec api env | grep -E "(DEBUG|DB_)"
```

**Veza s bazom podataka**:
```bash
# Provjera zdravlja baze podataka
docker-compose exec db pg_isready -U cijene_user -d cijene

# Prikaz logova baze podataka
docker-compose logs db
```

**Zdravlje servisa**:
```bash
# Provjera statusa svih servisa
docker-compose ps

# Test API health endpoint-a
curl http://localhost:8000/health
```

### Analiza logova

```bash
# Prikaz svih logova
docker-compose logs

# Praćenje specifičnog servisa
docker-compose logs -f api

# Provjera zadnjih 50 linija
docker-compose logs --tail=50 db
```

## Razmotrbe performansi

### Korištenje resursa

Konfiguracija pruža efikasno korištenje resursa:
- **Minimalne osnovne slike**: Alpine Linux za PostgreSQL
- **Višefazni build-ovi**: Manje produkcijske slike
- **Dijeljeni osnovni slojevi**: Efikasna pohrana slika

### Razmotrbe skaliranja

Za produkcijsko skaliranje:
- Konfiguriran database connection pooling
- Stateless API dizajn
- Odvojen model izvršavanja crawler-a
- Pohrana podataka na temelju volumena

## Održavanje

### Ažuriranja

```bash
# Ažuriranje osnovnih slika
docker-compose pull

# Ponovno buildanje slika aplikacije
docker-compose build --pull

# Ažuriranje i restart
docker-compose up -d --build
```

### Sigurnosno kopiranje

```bash
# Sigurnosno kopiranje baze podataka
docker-compose exec db pg_dump -U cijene_user cijene > backup.sql

# Sigurnosno kopiranje volumena
docker run --rm -v cijene-api_postgres_data:/data -v $(pwd):/backup alpine tar czf /backup/postgres_data.tar.gz -C /data .
```

### Čišćenje

```bash
# Uklanjanje zaustavljenih kontejnera
docker-compose down

# Uklanjanje volumena (⚠️ GUBITAK PODATAKA)
docker-compose down -v

# Čišćenje nekorištenih resursa
docker system prune
```