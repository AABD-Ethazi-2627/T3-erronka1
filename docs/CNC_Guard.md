# CNC Guard

## Aurkibidea

- [1. Sarrera eta arkitektura orokorra](#1-sarrera-eta-arkitektura-orokorra)
  - [1.1. Proiektuaren helburua eta esparru teknikoa](#11-proiektuaren-helburua-eta-esparru-teknikoa-cnc-guard)
  - [1.2. Arkitektura globalaren diagrama eta datu-fluxua](#12-arkitektura-globalaren-diagrama-eta-datu-fluxua)
  - [1.3. Azpiegitura edukiontziratua](#13-azpiegitura-edukiontziratua-docker-eta-docker-compose)
  - [1.4. Fitxategi partekatuaren konfigurazioa eta muntaketa](#14-fitxategi-partekatuaren-konfigurazioa-eta-muntaketa-cifs--nfs)
- [2. Datu-ingestioa eta streaming azpiegitura](#2-datu-ingestioa-eta-streaming-azpiegitura)
  - [2.1. Datuen jatorria eta sentsoreen formatua](#21-datuen-jatorria-eta-sentsoreen-formatua-csv)
  - [2.2. Filebeat: konfigurazioa, iragazketa eta egoeraren erregistrua](#22-filebeat-konfigurazioa-iragazketa-eta-egoeraren-erregistrua)
  - [2.3. Apache Kafka: broker-a, topic-ak, partizioak eta kontsumitzaile-taldeak](#23-apache-kafka-broker-a-topic-ak-cnc-sensors-raw-partizioak-eta-kontsumitzaile-taldeak)
  - [2.4. Apache NiFi: hasierako ingestio-flow-a eta Controller Service-ak](#24-apache-nifi-hasierako-ingestio-flow-a-eta-controller-service-ak)
- [3. Datuen azterketa esploratzailea (EDA) eta aurreprozesatzea](#3-datuen-azterketa-esploratzailea-eda-eta-aurreprozesatzea)
  - [3.1. Entrenamendu-datasetaren azterketa](#31-entrenamendu-datasetaren-azterketa-ai4i2020_traincsv)
  - [3.2. Azterketa esploratzailea eta datu-banaketak](#32-azterketa-esploratzailea-eda-eta-datu-banaketak)
  - [3.3. Datuen garbiketa eta data leakage-aren prebentzioa](#33-datuen-garbiketa-eta-datu-ihesaren-data-leakage-prebentzioa)
  - [3.4. Kategoria-aldagaien kodetzea](#34-kategoria-aldagaien-kodetzea-one-hot-encoding)
  - [3.5. Ezaugarri eratorrien sorrera](#35-ezaugarri-eratorrien-sorrera-feature-engineering)
- [4. Machine Learning ereduaren entrenamendua eta ebaluazioa](#4-machine-learning-ereduaren-entrenamendua-eta-ebaluazioa)
  - [4.1. Entrenamendu- eta balidazio-multzoen banaketa](#41-entrenamendu--eta-balidazio-multzoen-banaketa-estrategikoa)
  - [4.2. Klase-desoreka tratatzeko estrategiak](#42-klase-desoreka-tratatzeko-estrategiak-class-weight-eta-smote)
  - [4.3. Algoritmoen entrenamendua](#43-algoritmoen-entrenamendua-random-forest-vs-xgboost)
  - [4.4. Balidazio gurutzatua](#44-balidazio-gurutzatua-stratified-k-fold-cross-validation)
  - [4.5. Ereduen arteko konparazioa eta metrikak](#45-ereduen-arteko-konparazioa-eta-metrikak-precision-recall-f1-roc-auc)
  - [4.6. Atalasearen doikuntza eta kostu ekonomikoa](#46-atalasearen-doikuntza-threshold-tuning-eta-kostu-ekonomikoaren-azterketa)
  - [4.7. Ezaugarrien garrantzia eta ereduaren biltegiratzea](#47-ezaugarrien-garrantzia-feature-importance-eta-ereduaren-biltegiratzea-pkl)
- [5. Scoring APIa eta ML inferentzia zerbitzua](#5-scoring-apia-eta-ml-inferentzia-zerbitzua)
  - [5.1. FastAPI bidezko mikrozerbitzuaren arkitektura](#51-fastapi-bidezko-mikrozerbitzuaren-arkitektura-eta-egitura)
  - [5.2. Ereduaren karga eta Pydantic balidazioa](#52-ereduaren-karga-dinamikoa-eta-pydantic-balidazioa-sentsoresaria)
  - [5.3. Ezaugarri eratorriak eta ezaugarri-ordena](#53-ezaugarri-eratorrien-kalkulua-eta-ezaugarri-ordenaren-koherentzia)
  - [5.4. Atalase operatiboa eta alerta-mailak](#54-atalase-operatiboa-eta-alerta-mailen-definizioa-ok-kontuz-larria)
  - [5.5. APIaren autentifikazioa eta segurtasuna](#55-apiaren-autentifikazioa-eta-segurtasuna-x-api-key)
  - [5.6. Dockerizazioa eta egiaztapen-probak](#56-dockerizazioa-eta-egiaztapen-probak-curl-health-eta-predict)
- [6. Denbora errealeko pipelinearen integrazioa eta alerta-sistema](#6-denbora-errealeko-pipelinearen-integrazioa-eta-alerta-sistema)
  - [6.1. NiFi Streaming Middleware-aren flow-a](#61-nifi-streaming-middleware-aren-flow-osoaren-diseinua)
  - [6.2. Datuen eraldaketa fisikoa](#62-datuen-eraldaketa-fisikoa-kelvin--celsius-bihurketa)
  - [6.3. NiFi eta Scoring APIaren arteko komunikazioa](#63-nifi-eta-scoring-apiaren-arteko-komunikazioa-invokehttp)
  - [6.4. Aurreikuspenen bideratzea eta Elasticsearch biltegiratzea](#64-aurreikuspenen-bideratze-dinamikoa-eta-indize-anitzeko-biltegiratzea-elasticsearch-en)
  - [6.5. Telegram bidezko alerta-sistema](#65-alerta-sistema-automatizatua-telegram-bidez-kontuz-eta-larria)
- [7. Ustiapena, bistaratzea eta monitorizazioa Kibana-n](#7-ustiapena-bistaratzea-eta-monitorizazioa-kibana-n)
  - [7.1. Kibana Data View eta Index Pattern](#71-kibana-data-view-eta-index-pattern-en-konfigurazioa)
  - [7.2. Panela eta bistaratzeak](#72-panela-eta-bistaratzeak-kpiak-line-chart-ak-heatmap-ak-eta-gertaera-taulak)
  - [7.3. Denbora errealeko monitorizazioa](#73-denbora-errealeko-monitorizazioa-eta-sentsoreen-jarraipena)
- [8. Ondorioak eta etorkizuneko hobekuntzak](#8-ondorioak-eta-etorkizuneko-hobekuntzak)
  - [8.1. Lortutako emaitzak eta betetako helburuak](#81-lortutako-emaitzen-laburpena-eta-betetako-helburuak)
  - [8.2. Muga teknikoak](#82-sistematik-hautemandako-muga-teknikoak)
  - [8.3. Etorkizuneko lan-ildoak](#83-etorkizuneko-lan-ildoak-mlops-ereduaren-birentrenamendua-eta-ezaugarri-berriak)

## 1. SARRERA ETA ARKITEKTURA OROKORRA

### 1.1. Proiektuaren helburua eta esparru teknikoa (CNC Guard)

CNC Guard sistema adimenduna da, CNC makinen sentsore-datuak denbora errealean jaso, prozesatu eta aztertzeko diseinatua. Sistemaren helburu nagusia makinen balizko matxurak edo portaera anomaloak aurrez hautematea da, mantentze-lanak planifikatu eta geldialdi ezustekoak murrizteko.

Horretarako, datuak CSV fitxategietatik hartu eta Filebeat, Kafka eta NiFi erabiliz streaming-pipeline batean garraiatzen dira. Ondoren, Machine Learning ereduak datuak aztertu eta arrisku-maila kalkulatzen du; emaitzak Elasticsearch-en gordetzen dira eta Kibana-n bistaratzen dira. Alerta-maila larrien kasuan, sistemak jakinarazpenak bidaltzen ditu Telegram bidez.

Proiektuaren esparru teknikoak honako osagai hauek biltzen ditu: Docker bidezko azpiegitura edukiontziratua, datu-ingestioa eta streaming-a, datuen aurreprozesatzea, ML ereduaren entrenamendua, FastAPI bidezko inferentzia-zerbitzua, Elasticsearch eta Kibana bidezko monitorizazioa, eta alerta automatizatuak.

### 1.2. Arkitektura globalaren diagrama eta datu-fluxua
Atal honetan proiektuaren funtzionamendu orokorra eta informazioaren ibilbidea azaltzen dira. 

Jarraian agertzen den diagramak (Mermaid bidez diseinatua) sistemaren osagai nagusiak, haien arteko elkarrekintzak eta datu-fluxua modu bisual eta argian erakusten ditu. Irudikapen honi esker, sistemaren prozesu osoa –hasierako interakziotik hasi eta amaierako emaitzara arte– errazago ulertu ahal izango da.
```mermaid
flowchart LR
    A["Simuladorea (CSV)"] -->|Samba / NFS| B["Filebeat"]
    B -->|cnc-sensors-raw| C["Apache Kafka"]
    C --> D["Apache NiFi"]
    D -->|Datu gordinak| E[("Elasticsearch")]
    D -->|Sentsoreak /predict| F["Scoring API (FastAPI)"]
    F -->|Probabilitatea & Alerta| D
    D -->|cnc-alerts / cnc-normal| E
    E --> G["Kibana Dashboards"]
```

### 1.3. Azpiegitura edukiontziratua (Docker eta Docker Compose)
#### Docker Compose-ren helburua
CNC Guard proiektuan Docker eta Docker Compose erabili dira sistemaren azpiegitura osoa edukiontzien bidez hedatzeko eta kudeatzeko. Horri esker, proiektuko zerbitzu guztiak ingurune isolatu eta independenteetan exekuta daitezke, bakoitzaren mendekotasunak eta konfigurazioak kontrolatuz.

docker-compose.yml fitxategiak sistemaren zerbitzu guztiak definitzen ditu, baita haien arteko komunikazioa, sare-konfigurazioa, portuak, ingurune-aldagaiak, bolumen iraunkorrak eta abiarazte-mendekotasunak ere.

Konfiguratutako azpiegiturak honako zerbitzu hauek biltzen ditu:
| Zerbitzua | Batezbestekoa 
|---|---|
| Apache Kafka | Sentsoreen datuak bufferizatu eta streaming bidez banatzea. |
| Filebeat | Fitxategi partekatuko datuak irakurri eta Kafkara bidaltzea. |
| Apache NiFi | Datuen ingestioa, eraldaketa eta bideratze automatikoa. |
| Elasticsearch | Datuak eta aurreikuspenen emaitzak biltegiratzea eta indexatzea. |
| Kibana | Datuak bistaratzea eta monitorizazio-panelak sortzea. |
| Scoring API | Machine Learning ereduaren inferentziak eskaintzea FastAPI bidez. |

#### Filebeat — Fitxategien irakurketa eta bidalketa

Filebeat-ek fitxategi partekatuan sortzen diren sentsoreen datuak irakurtzen ditu eta konfiguratutako irteeraren bidez Kafkara bidaltzen ditu.

- `filebeat.yml`: irakurketa-iturburua, datuen iragazketa eta Kafka-rako konexioa definitzen ditu.
- `user: root`: fitxategi partekatuko edukia irakurtzeko beharrezko baimenak izatea ahalbidetzen du.
- `--strict.perms=false`: konfigurazio-fitxategiaren baimenei buruzko egiaztapen zorrotza desgaitzen du.
- `filebeat-data`: irakurritako fitxategien egoera eta aurrerapena gordetzen ditu, berrabiarazteetan prozesamendua kontrolatzeko.
- `cnc-stream-cifs`: CIFS bidez muntatutako fitxategi partekatua irakurtzeko erabiltzen da.

#### Apache NiFi — Datuen eraldaketa eta bideratzea
NiFi datuen streaming pipeline-aren koordinazioaz arduratzen da. Kafkatik jasotako mezuak prozesatu, beharrezko eraldaketak egin, Scoring APIarekin komunikatu eta emaitzak Elasticsearch-era bidaltzen ditu.

- `8443:8443`: NiFi-ren HTTPS web-interfazea eskuragarri jartzen du.
- `SINGLE_USER_CREDENTIALS_USERNAME` eta `SINGLE_USER_CREDENTIALS_PASSWORD`: web-interfazeko erabiltzailearen autentifikazioa konfiguratzen dute.
- `NIFI_WEB_HTTPS_PORT`: HTTPS bidezko sarbidearen portua definitzen du.
- `depends_on`: Kafka eta Elasticsearch zerbitzuak NiFi baino lehen abiaraztea ezartzen du.

#### Elasticsearch — Datuen biltegiratzea
Elasticsearch sistemaren biltegiratze eta bilaketa-motorra da. Sentsoreen erregistroak eta Machine Learning ereduak sortutako aurreikuspenak indexatzeko erabiltzen da.

- `discovery.type=single-node`: nodo bakarreko konfigurazioa erabiltzen du.
- `xpack.security.enabled=true`: autentifikazio- eta segurtasun-funtzioak aktibatzen ditu.
- `ELASTIC_PASSWORD`: administratzailearen pasahitza ezartzen du.
- `ES_JAVA_OPTS`: JVM-rako memoria-mugak definitzen ditu.
- `es-data`: datuen iraunkortasuna bermatzen du, edukiontzia berrabiarazi edo ordezkatu arren.

#### Kibana — Datuen bistaratzea
Kibanak Elasticsearch-en gordetako informazioa grafikoki aztertzeko aukera ematen du. Monitorizazio-panelak, KPIak, denbora-serieak, heatmap-ak eta gertaeren taulak sortzeko erabiltzen da.

- `ELASTICSEARCH_HOSTS`: Elasticsearch zerbitzuaren helbidea definitzen du.
- `ELASTICSEARCH_USERNAME`: Kibana-ren eta Elasticsearch-en arteko komunikaziorako erabiltzailea zehazten du.
- `ELASTICSEARCH_PASSWORD`: erabiltzaile horren pasahitza definitzen du.
- `XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY`: Kibana-k objektu sentikorrak enkriptatzeko erabiltzen duen gakoa definitzen du.
- `5601:5601`: Kibana-ren web-interfazea eskuragarri jartzen du.

#### Scoring API — Machine Learning inferentzia
Scoring APIa FastAPI erabiliz garatutako mikrozerbitzua da. Aurretik entrenatutako Machine Learning eredua kargatzen du eta NiFIk bidalitako sentsoreen datuekin hutsegiteen probabilitatea kalkulatzen du.

- `build.context`: APIaren Docker irudia eraikitzeko erabiliko den direktorioa definitzen du.
- `8000:8000`: APIaren HTTP portua kanpora irekitzen du.
- `./models:/models:ro`: entrenatutako eredua edukiontzian muntatzen du, irakurtzeko soilik.
- `MODEL_PATH`: erabiliko den `.pkl` ereduaren kokapena definitzen du.
- `CORS_ORIGINS`: APIra konektatzeko baimendutako jatorriak zehazten ditu.
- `API_KEY_SECRET`: APIaren autentifikaziorako erabilitako gako sekretua definitzen du.

#### Bolumen iraunkorrak eta fitxategi partekatua
Docker-en bolumenak erabiltzen dira edukiontziak berrabiarazi arren beharrezko informazioa mantentzeko.

Proiektuan hiru bolumen nagusi definitu dira:

- `filebeat-data`: Filebeat-en egoera eta irakurketa-aurrerapena gordetzeko.
- `es-data`: Elasticsearch-en indexatutako informazioa gordetzeko.
- `cnc-stream-cifs`: sareko fitxategi partekatua CIFS protokoloaren bidez muntatzeko.

Azken bolumen horrek `192.168.75.150` zerbitzarian kokatutako `cnc-stream` direktorio partekatua erabiltzen du. `ro` aukerak irakurtzeko soilik muntatzen dela adierazten du.

#### Edukiontzien abiaraztea eta kudeaketa
**Zerbitzu guztiak batera abiarazteko:**
```bash
docker compose up -d --build
```

**Edukiontzien egoera ikusteko:**
```bash
docker compose ps
```

**Zerbitzu jakin baten log-ak ikusteko:**
```bash
docker compose logs -f kafka
```

**Zerbitzu guztiak gelditzeko:**
```bash
docker compose down
```
### 1.3.1. Ingurune-aldagaien konfigurazioa (`.env`)

Docker Compose-ren konfigurazioan kredentzialak eta informazio sentikorra zuzenean idatzi beharrean, ingurune-aldagaiak erabiltzen dira. Horretarako, `.env` fitxategia erabiltzen da.

Fitxategi horretan zerbitzuen konfigurazio pribatuak gordetzen dira, hala nola:

- NiFi-ren erabiltzaile-izena eta pasahitza.
- Elasticsearch-en administratzailearen pasahitza.
- Kibana-ren enkriptatze-gakoa.
- Scoring APIaren autentifikazio-gakoa.
- CIFS sare-partekatuaren erabiltzaile-izena eta pasahitza.

Docker Compose-k automatikoki irakurtzen ditu `.env` fitxategiko aldagaiak eta `docker-compose.yml` fitxategian `${ALDAGAIA}` sintaxiaren bidez erabiltzeko aukera ematen du.

#### `.env.example` fitxategia

Taldekide guztiek proiektua exekutatu ahal izateko, `.env.example` fitxategia gordeko da Git biltegian. Fitxategi horrek behar diren aldagai guztiak definituko ditu, baina benetako pasahitzik gabe.

Adibidez:

```env
# Apache NiFi
NIFI_USER=admin
NIFI_PASSWORD=CHANGE_ME_NIFI_PASSWORD

# Elasticsearch
ELASTIC_PASSWORD=CHANGE_ME_ELASTIC_PASSWORD

# Kibana
KIBANA_SYSTEM_PASSWORD=CHANGE_ME_KIBANA_PASSWORD
KIBANA_ENCRYPTION_KEY=CHANGE_ME_WITH_32_CHARACTERS_MINIMUM

# Scoring API
API_KEY_SECRET=CHANGE_ME_API_SECRET

# CIFS shared folder
CIFS_USERNAME=CHANGE_ME_CIFS_USER
CIFS_PASSWORD=CHANGE_ME_CIFS_PASSWORD
```
#### `.gitignore` fitxategia
Benetako .env fitxategia Git-etik kanpo utzi behar da, kredentzialak publikoki ez zabaltzeko.

Horretarako, .gitignore fitxategian honako lerro hauek gehituko dira:
```gitignore
# Environment variables and secrets
.env
.env.*
!.env.example
```

#### Taldekideen konfigurazioa
Taldekide bakoitzak bere .env fitxategia sortuko du .env.example fitxategitik abiatuta:
```bash
cp .env.example .env
```
Ondoren, dagokion ingurunean behar diren kredentzialak eta konfigurazio-balioak sartuko ditu.

Horrela, proiektuaren konfigurazio-egitura partekatzen da, baina kredentzial pertsonalak eta informazio sentikorra ez dira Git biltegian gordetzen.

**Garrantzitsua:** `.env.example` fitxategiak ez ditu benetako pasahitzak izan behar. Hala ere, zuen azpiegiturak CIFS partekatutako karpeta eta gainerako zerbitzuak behar dituenez, exekutatzeko benetako kredentzialak modu seguruan partekatu beharko dituzue, adibidez, kanal pribatu baten bidez.
### 1.4. Fitxategi partekatuaren konfigurazioa eta muntaketa (CIFS / NFS)

## 2. DATU-INGESTIOA ETA STREAMING AZPIEGITURA

### 2.1. Datuen jatorria eta sentsoreen formatua (CSV)

CNC Guard proiektuak **AI4I 2020 Predictive Maintenance Dataset** datu-multzoa erabiltzen du oinarri gisa. Datu-multzo horrek industria-ingurune bateko CNC makinen funtzionamendua eta sentsoreen neurketak simulatzen ditu, makinen hutsegiteak aurreikusteko eta mantentze prediktiboa lantzeko.

Datuak CSV (*Comma-Separated Values*) formatuan gordetzen dira. Fitxategi bakoitzaren lerro batek neurketa edo erregistro bat adierazten du, eta zutabe bakoitzak makinaren egoerari, funtzionamendu-parametroei edo hutsegiteei buruzko informazioa jasotzen du.

CSV fitxategiaren egitura honako hau da:

```text
timestamp,machine_id,machine_type,air_temp_K,process_temp_K,rpm,torque_Nm,tool_wear_min,machine_failure,failure_TWF,failure_HDF,failure_PWF,failure_OSF,failure_RNF
```

**Zutabeen deskribapena:**

| Zutabea | Deskribapena | Datu mota / Unitatea |
|---|---|---|
| `timestamp` | Neurketa egin den data eta ordua adierazten ditu. | Datetime (UTC) |
| `machine_id` | Neurketa egin duen makinaren identifikatzaile bakarra. | String |
| `machine_type` | Makinaren mota edo kategoria adierazten du (L, M edo H). | Kategorikoa |
| `air_temp_K` | Inguruko airearen tenperatura adierazten du. | Float (Kelvin) |
| `process_temp_K` | Makina-prozesuaren tenperatura adierazten du. | Float (Kelvin) |
| `rpm` | Makinaren biraketa-abiadura adierazten du. | Integer (RPM) |
| `torque_Nm` | Makinaren ardatzean aplikatutako momentu eragilea adierazten du. | Float (N·m) |
| `tool_wear_min` | Erremintak erabilitako denbora metatua adierazten du. | Integer (minutuak) |
| `machine_failure` | Makinak hutsegiterik izan duen adierazten du. | Boolean (0/1) |
| `failure_TWF` | Erremintaren higadurarekin lotutako hutsegitea (Tool Wear Failure). | Boolean (0/1) |
| `failure_HDF` | Beroaren disipazioarekin lotutako hutsegitea (Heat Dissipation Failure). | Boolean (0/1) |
| `failure_PWF` | Potentzia-prozesuarekin lotutako hutsegitea (Power Failure). | Boolean (0/1) |
| `failure_OSF` | Gehiegizko tentsio mekanikoarekin lotutako hutsegitea (Overstrain Failure). | Boolean (0/1) |
| `failure_RNF` | Ausazko hutsegitea (Random Failure). | Boolean (0/1) |

Hutsegiteen adierazleek balio bitarrak erabiltzen dituzte: `0` balioak hutsegiterik ez dagoela adierazten du, eta `1` balioak, berriz, dagokion hutsegite mota gertatu dela.

`machine_failure` aldagaia Machine Learning ereduaren **helburu-aldagaia (target variable)** da; hau da, ereduak makina baten hutsegitea gertatuko den aurreikusteko erabiltzen duen aldagaia. Gainerako `failure_*` zutabeek hutsegite mota zehazten dute, eta datuen azterketan eta ebaluazioan erabil daitezke.

Bestalde, `machine_id` eta `machine_type` aldagaiek makinak identifikatzeko eta sailkatzeko balio dute. Tenperatura, biraketa-abiadura, momentu eragilea eta erremintaren higadura, berriz, makinaren egoera operatiboa deskribatzen duten sentsore-ezaugarriak dira.

**Datuen prozesamenduan kontuan hartu beharreko alderdiak:**

- Tenperaturak Kelvin-etan jasotzen dira jatorrizko datuetan. Streaming pipeline-an Celsius graduetara bihurtzen dira, monitorizazioa eta interpretazioa errazteko.
- `rpm`, `torque_Nm` eta `tool_wear_min` aldagaiek makinaren egoera mekanikoa eta erabilera-maila deskribatzen dituzte.
- `machine_type` aldagai kategorikoa da, eta entrenamendu-fasean kodetu behar da ereduak erabil dezan.
- Hutsegiteen adierazleak datu historikoen analisirako erabiltzen dira. Inferentzia-fasean, ordea, ereduak sentsoreen neurketetatik abiatuta kalkulatzen du hutsegitearen probabilitatea.
- Datuen ingestioan, erregistro bakoitzaren denbora-zigilua eta makinaren identifikatzailea mantentzen dira, ondorengo monitorizazioa eta trazabilitatea bermatzeko.

CSV formatua aukeratu da datuak modu sinple eta egituratuan gordetzeko, baita Filebeat, Apache Kafka eta Apache NiFi bezalako tresnekin erraz integratzeko ere.

### 2.2. Filebeat: konfigurazioa, iragazketa eta egoeraren erregistrua
Proiektu honetan, Filebeat datuak biltzeko eragile arin gisa (lightweight shipper) erabiltzen da. Bere eginkizun nagusia sentsoreek sortutako CSV fitxategiak irakurtzea eta, ia denbora errealean, Apache Kafkara bidaltzea da, datuen prozesamendu-fluxuarekin jarraitzeko.

### 2.3. Apache Kafka: broker-a, topic-ak (`cnc-sensors-raw`), partizioak eta kontsumitzaile-taldeak
Proiektu honetan, Apache Kafka datuen ingestio-fluxuaren ardatz nagusia edo *broker*-a da. Bere funtzio nagusia sentsoreen datu gordinak denbora errealean jasotzea eta prozesamendu-sistemek (Apache NiFi) kontsumitu ditzaten eskuragarri jartzea da.

Datuen jarioa honela egituratzen da Kafkaren inguruan:

- **Datuen sarrera (Filebeat):** Filebeat tresna konfiguratuta dago sentsoreen CSV datuak irakurri eta Kafkako `cnc-sensors-raw` izeneko *topic*-era bidaltzeko.
- **Datuen irteera (NiFi):** Apache NiFi-k `ConsumeKafka` prozesadorea erabiltzen du *topic* horretatik datuak irakurtzeko. Irakurketa hau `cnc-nifi` izeneko kontsumitzaile-taldearen (Group ID) bidez egiten da, mezuak behin bakarrik eta modu ordenatuan prozesatzen direla bermatzeko.

#### Topic-aren sorrera

Sentsoreen informazioa bideratzeko `cnc-sensors-raw` *topic*-a sortu behar da. Egitura honetan 3 partizio definitu dira datuen irakurketa paraleloa ahalbidetzeko, eta erreplikazio-faktorea 1 gisa utzi da, nodo bakarreko kluster batean baikaude.

Horretarako, honako komando hau exekutatu dugu terminalean (Docker edukiontziaren kanpotik):

```bash
# Terminala abiarazi
docker exec -it cnc-kafka bash
```

```bash
kafka-topics --create --topic cnc-sensors-raw --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
```

#### Konfigurazioaren egiaztapena
Topic-a behar bezala sortu dela eta partizio nahiz erreplikazio datuak zuzenak direla egiaztatzeko, describe komandoa erabili da:
```bash
kafka-topics --describe --topic cnc-sensors-raw --bootstrap-server localhost:9092
```
Komando honek topic-aren egoera orokorra eta partizio bakoitzaren nodo liderra zein den erakusten du, dena prest dagoela ziurtatuz ingestioarekin hasteko.

### 2.4. Apache NiFi: hasierako ingestio-flow-a eta Controller Service-ak
Proiektu honetan Apache NiFi erabili dugu datuen fluxu osoa kudeatzeko, sentsoreetatik Elasticsearch-era eta Machine Learning eredura bideratuz. Bi fluxu nagusi diseinatu dira (1. fasea eta 3. fasea), baina **3. faseko fluxuak 1. faseko ingestio-prozesu bera biltzen du bere baitan**. 

Jarraian, prozesu osoaren arkitektura eta osagaiak zehazten dira.

### *1. Ingestio-fluxuaren oinarria (1. Fasea)*

3. faseko fluxuaren lehen zatia hasierako ingestio-fluxuaren (*fase1_ingest.json*) kopia zehatza da. Helburua Kafkatik datu gordinak irakurri eta Elasticsearch-en gordetzea da. Urrats hauek jarraitzen ditu:

1.  **`ConsumeKafka`:** Kafkako `cnc-sensors-raw` *topic*-era harpidetzen da datuak denbora errealean jasotzeko.
2.  **`ConvertRecord`:** Kafkatik jasotako CSV formatuko datuak JSON formatura bihurtzen ditu, prozesamendua errazteko (`CSVReader` eta `JsonRecordSetWriter` erabiliz).
3.  **`RouteOnAttribute`**: Iragazki sinple bat aplikatzen du datu hutsak edo baliogabeak baztertzeko (`${fileSize:gt(0)}`).
4.  **`PutElasticsearchJson`**: Datu gordinak (*raw*) Elasticsearch-eko `cnc-sensors` indizean gordetzen ditu zuzenean.

### *2. Streaming eta Iragarpen Fluxu Aurreratua (3. Fasea)*

Behin datu gordinak prozesatuta eta gordeta (aurreko atalean bezala), fluxuak bide berri bat irekitzen du datu horiek denbora errealean Machine Learning API-ra bidaltzeko eta emaitzen arabera ekintzak burutzeko (*fase3_streaming.json*).

Fase honetako prozesadore nagusiak hauek dira:

#### A) Datuen prestaketa API-rako
*   **`EvaluateJsonPath`**: Datuen fluxutik beharrezko eremuak ateratzen ditu (`machine_id`, `machine_type`, `timestamp`) eta atributu gisa gordetzen ditu.
*   **`JoltTransformJSON`**: JSON mezuaren egitura eraldatzen du, Scoring API-ak (FastAPI) eskatzen duen formatu zehatzera egokituz.

#### B) Machine Learning Ereduari deia
*   **`InvokeHTTP` (Scoring API)**: API-ari `POST` eskaera bat egiten dio datuekin (`http://scoring-api:8000/predict`). Segurtasunerako `X-API-Key` bat bidaltzen du goiburukoan.
*   **`EvaluateJsonPath`**: API-ak itzultzen duen erantzunetik iragarpenaren emaitzak erauzten ditu: `failure_predicted`, `failure_probability` eta `alert_level`.

#### C) Bideratzea eta Biltegiratzea (Emaitzen arabera)
Iragarpenaren emaitza aztertu ondoren, **`RouteOnAttribute`** prozesadore baten bidez (egoeraren arabera) fluxua hainbat adarretan banatzen da:

*   **`PutElasticsearchJson OK`**: Makinak ondo funtzionatzen badu, datuak `cnc-prediction2` indizean gordetzen dira.
*   **`PutElasticsearchJson FALLOS`**: Makinan anomalia bat edo akats bat aurreikusten bada, informazioa `cnc-error2` indizean gordetzen da.
*   **`UpdateRecord` & `PutElasticsearchJson (Silver)`**: Tenperaturak Kelvinetik Celsius gradutara eraldatzen dira eta datu garbiagoak `cnc-silver` indizean gordetzen dira, gerora analisi historikoa egiteko.

#### D) Alerta Sistema (Telegram)
API-ak makina baten hutsegite arriskua detektatzen badu, denbora errealeko abisuak bidaltzen dira mantentze-taldeko Telegram kanalera:

*   **`ReplaceText KONTUZ` / `LARRIA`**: Iragarritako alerta-mailaren arabera (KONTUZ edo LARRIA), Telegramerako mezu formatu berezi bat (Markdown) sortzen du, makinaren ID-a, data eta egoera txertatuz.
*   **`InvokeHTTP TELEGRAM`**: Telegramen API ofizialera deitzen du bot baten bidez (`/sendMessage`), abisua mantentze-taldearen txatean berehala ager dadin.
## 3. DATUEN AZTERKETA ESPLORATZAILEA (EDA) ETA AURREPROZESATZEA

### 3.1. Entrenamendu-datasetaren azterketa (`ai4i2020_train.csv`)

### 3.2. Azterketa esploratzailea (EDA) eta datu-banaketak

### 3.3. Datuen garbiketa eta datu-ihesaren (data leakage) prebentzioa

### 3.4. Kategoria-aldagaien kodetzea (One-Hot Encoding)

### 3.5. Ezaugarri eratorrien sorrera (Feature Engineering)

## 4. MACHINE LEARNING EREDUAREN ENTRENAMENDUA ETA EBALUAZIOA

### 4.1. Entrenamendu- eta balidazio-multzoen banaketa estrategikoa

### 4.2. Klase-desoreka tratatzeko estrategiak (Class Weight eta SMOTE)

### 4.3. Algoritmoen entrenamendua: Random Forest vs XGBoost

### 4.4. Balidazio gurutzatua (Stratified K-Fold Cross-Validation)

### 4.5. Ereduen arteko konparazioa eta metrikak (Precision, Recall, F1, ROC-AUC)

### 4.6. Atalasearen doikuntza (Threshold Tuning) eta kostu ekonomikoaren azterketa

### 4.7. Ezaugarrien garrantzia (Feature Importance) eta ereduaren biltegiratzea (`.pkl`)

## 5. SCORING APIA ETA ML INFERENTZIA ZERBITZUA
Proiektu honen bihotz prediktiboa Scoring API-a da. FastAPI bidez eraikitako mikrozerbitzu honek sentsoreen datu gordinak jasotzen ditu, denbora errealean prozesatzen ditu eta entrenatutako Machine Learning ereduaren (Random Forest) bidez hutsegite-probabilitatea itzultzen du. Emaitza hauek NiFi-k (3. fasean) erabiltzen ditu alertak bideratzeko.

### 5.1. FastAPI bidezko mikrozerbitzuaren arkitektura eta egitura
APIa **FastAPI** framework-aren bidez garatu da, bere errendimendu altuagatik eta asinkronismoa kudeatzeko duen erraztasunagatik ezaguna. Zerbitzari gisa **Uvicorn** erabiltzen da.
Arkitektura RESTful ereduan oinarritzen da, eta mikrozerbitzu isolatu gisa pentsatuta dago. Horrek esan nahi du bere ardura bakarra (Single Responsibility Principle) inferentzia egitea dela. 

Kodean CORS (Cross-Origin Resource Sharing) middleware bat ezarri da (`CORSMiddleware`), baimendutako jatorrietatik (`localhost:3000` edo ingurune-aldagai bidez definitutakoetatik) soilik onartzeko eskaerak. Gainera, prozesamendu-denbora neurtzeko middleware pertsonalizatu bat ere barneratu da (`X-Process-Time` goiburukoa itzuliz), inferentziaren latentzia denbora errealean monitorizatzeko.

### 5.2. Ereduaren karga dinamikoa eta Pydantic balidazioa (`SentsoreSaria`)
fizientzia bermatzeko, Machine Learning eredua (`cnc_failure_model.pkl`) ez da eskaera bakoitzean kargatzen. FastAPI-ren `@asynccontextmanager` eta `lifespan` funtzioak erabiliz, eredua memorian behin bakarrik kargatzen da aplikazioa abiaraztean. 

Datuen kalitatea bermatzeko, **Pydantic** liburutegia erabili da datuen balidaziorako. `SentsoreSaria` klaseak (BaseModel) sarrerako datuen egitura eta mugak zehazten ditu zorrozki:
* **Formatu-balidazioak:** `machine_id` eremuak patroi zehatz bat bete behar du (`^CNC-[0-9]{3}$`), eta `machine_type`-ak 'L', 'M' edo 'H' izan behar du.
* **Muga fisikoen balidazioak:** Sentsoreen balioek muga logikoak izan behar dituzte (`rpm` 0 eta 10000 artean, tenperaturak 200.0 eta 400.0 artean, etab.).

Honek sistemaren babes-lehen-lerro gisa funtzionatzen du: datu anomaloak edo ustelak (adibidez, tenperatura negatibo ezinezko bat) eredura iritsi aurretik baztertzen dira, "Zaborra sartu, zaborra atera" (Garbage In, Garbage Out) arazoa ekidinez.

### 5.3. Ezaugarri eratorrien kalkulua eta ezaugarri-ordenaren koherentzia
Eredu prediktiboak ez ditu sentsoreen datu gordinak bakarrik erabiltzen. Inferentzia egin aurretik, `/predict` endpoint-ak *Feature Engineering* (ezaugarrien ingeniaritza) aplikatzen du Pandas erabiliz. Aldagai eratorri hauek kalkulatzen dira bertan, entrenamendu-fasean egin zen bezalaxe:
* `power`: `rpm` * `torque_Nm`
* `temp_diff`: `process_temp_K` - `air_temp_K`
* `torque_wear`: `torque_Nm` * `tool_wear_min`
* One-hot encoding `machine_type` aldagaiarentzat (`machine_type_L`, `machine_type_M`).

**Koherentzia kritikoa:** Eredu bat ustiatzean ematen den errore ohikoena zutabeen ordenaren aldaketa da. Kodean mekanismo bat ezarri da `model.feature_names_in_` erabiliz. Honek sarrerako DataFrame-ko zutabeak berrantolatzen ditu ereduak entrenamenduan ikasi zuen **ordena zehatzera**, eta falta den ezaugarririk balego (500 errorea emanez) abisatzen du. 

### 5.4. Atalase operatiboa eta alerta-mailen definizioa (OK, KONTUZ, LARRIA)
Ereduak `predict_proba()` bidez hutsegite bat gertatzeko *probabilitatea* itzultzen du (0.0 - 1.0 bitartean). Makina bat noiz gelditu erabakitzeko probabilitate hori kategoria bihurtu behar da.

* **Atalase nagusia (Threshold):** `0.40` (edo %40) ezarri da. Probabilitatea >= 0.40 bada, `failure_predicted = True` izango da. Atalase hau kontserbadorea da, industriako mantentze prediktiboan hobe baita aurreikuspen faltsu positibo bat (makina berrikustea ondo egon arren) makina baten apurketa garesti bat jasatea baino.
* **Alerta-mailak (`alert_level`):** Erakundeko komunikazioa (Telegram bidezkoa) kudeatzeko, probabilitatearen arabera hiru maila semaforiko sortu dira:
    * **OK:** prob < 0.40. Funtzionamendu normala.
    * **KONTUZ:** 0.40 <= prob <= 0.80. Berrikuspen programatua behar da.
    * **LARRIA:** prob > 0.80. Makinaren berehalako gelditzea eta ikuskapen kritikoa gomendatzen da.

### 5.5. APIaren autentifikazioa eta segurtasuna (`X-API-Key`)
#### Segurtasun Logikoa (X-API-Key)
API-ak informazio industriala jasotzen duenez, ez da publikoa izan behar. Segurtasuna bermatzeko `X-API-Key` goiburukoa ezarri da FastAPI-ren `Security` moduluaren bidez. Baimendutako sistemei (kasu honetan NiFi eta Telegram bot-en atzealdeari) pasahitza eskatzen zaie. Erantzuna ezeztatuko da (403 HTTP) gako hori zehatza ez bada.

#### Adimen Artifizialeko Ereduen Segurtasuna eta Etika
Sistema prediktibo industrial batek hainbat arau eta praktika on bete behar ditu AAren inplementazio seguru baterako:

1. **Security by Design (Diseinuzko Segurtasuna):**
   * *Data Validation (Poisoning ekiditea):* Pydantic-ek ezarritako mugek (adibidez, RPM 10000tik behera) ekiditen dute "Data Poisoning" edo sarrera gaiztoen erasoak, non erasotzaile batek datu izugarri handiak bidaliko lituzkeen eredua zoratu edo ukapen-zerbitzu (DoS) bat eragiteko.
   * *Ereduaren Karga Segurua:* `.pkl` (pickle) fitxategiak desestaltzea arriskutsua izan daiteke kode arbitrarioa exekuta dezaketelako. Hori ekiditeko, eredua bide lokal eta isolatu batetik soilik kargatzen da (`MODEL_PATH`), eta kanpoko inork ezin du modeloa gainidatzi Docker irudiaren baimenik gabe.

2. **Marko Legala eta Gardentasuna (AI Act / GDPR):**
   * EBko Adimen Artifizialaren Legeari (AI Act) jarraiki, industria-kontrolerako sistemak "arrisku handiko" (High-Risk) sailka daitezke. API honek `logging` bidezko erregistro sakona du. Eskaera bakoitza eta ereduaren erabakia erregistratzen dira, auditagarritasuna eta trazabilitatea ahalbidetuz. Ereduak erabaki "opaku" bat hartzen badu, zergatia Pandas-ek kalkulatutako ezaugarri zehatzetan (Power, Torque) mapa daiteke.

3. **Alborapenak (Biases):**
   * Makina mota ezberdinen artean (`L` - Low, `M` - Medium, `H` - High) alborapenik ez gertatzeko, eredua ezaugarri espezifiko hauek kontuan hartuz elikatu da (One-Hot Encoding bidez). Horrela, sistemak ez du makina indartsuago bat (H) akatstduntzat hartuko berezko tenperatura edo Torque balio altuagoak izateagatik, makinaren testuingurua ulertzen baitu.

### 5.6. Dockerizazioa eta egiaztapen-probak (`curl`, `/health` eta `/predict`)
APIa plataformarekiko independentea izan dadin, Docker bidez ontziratu da `python:3.11-slim` irudi arina erabiliz. Dockerfile-ak beharrezko mendekotasunak instalatzen ditu (`requirements.txt`) eta Uvicorn zerbitzaria abiarazten du 8000 portuan.

**Egiaztapen probak (curl):**

Ingurunea behar bezala dabilela eta ereduak kargatu direla ziurtatzeko, ondorengo deia egin daiteke (`/health`):
```bash
curl -X GET "http://localhost:8000/health" -H "X-API-Key: CNCGuard_Guadalquia_Key_2026_x89a01!"
```
Erantzun espero dena: `{"status":"ok","model_loaded":true,"model_path":"../models/cnc_failure_model.pkl"}`

Predikzio bat egiteko simulazioa (`/predict`):
```bash
curl -X POST "http://localhost:8000/predict" \
     -H "Content-Type: application/json" \
     -H "X-API-Key: CNCGuard_Guadalquia_Key_2026_x89a01!" \
     -d '{
           "timestamp": "2024-05-15 10:30:00",
           "machine_id": "CNC-042",
           "machine_type": "M",
           "air_temp_K": 298.1,
           "process_temp_K": 308.6,
           "rpm": 1551,
           "torque_Nm": 42.8,
           "tool_wear_min": 46
         }'
```
Erantzuna JSON formatuan itzuliko da, makinak akatsik aurreikusten duen, kalkulatutako probabilitatea zenbatekoa den eta dagokion alerta-maila azalduz.

## 6. DENBORA ERREALEKO PIPELINEAREN INTEGRAZIOA ETA ALERTA-SISTEMA

### 6.1. NiFi Streaming Middleware-aren flow osoaren diseinua

### 6.2. Datuen eraldaketa fisikoa (Kelvin → Celsius bihurketa)

### 6.3. NiFi eta Scoring APIaren arteko komunikazioa (`InvokeHTTP`)

### 6.4. Aurreikuspenen bideratze dinamikoa eta indize anitzeko biltegiratzea Elasticsearch-en

### 6.5. Alerta-sistema automatizatua Telegram bidez (KONTUZ eta LARRIA)

## 7. USTIAPENA, BISTARATZEA ETA MONITORIZAZIOA KIBANA-N

### 7.1. Kibana Data View eta Index Pattern-en konfigurazioa

### 7.2. Panela eta bistaratzeak (KPIak, line chart-ak, heatmap-ak eta gertaera-taulak)

### 7.3. Denbora errealeko monitorizazioa eta sentsoreen jarraipena

## 8. ONDORIOAK ETA ETORKIZUNEKO HOBEKUNTZAK

### 8.1. Lortutako emaitzen laburpena eta betetako helburuak

### 8.2. Sistematik hautemandako muga teknikoak

### 8.3. Etorkizuneko lan-ildoak (MLOps, ereduaren birentrenamendua eta ezaugarri berriak)
