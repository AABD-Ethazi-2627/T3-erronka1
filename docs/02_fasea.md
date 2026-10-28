# 2. Fasea --- Preprocessing eta Entrenamendua (Offline)
## Helburua

Datu historikoetatik abiatuta, makinen hutsegiteak aurreikusteko eredu
bat entrenatzea. Eredua `models/cnc_failure_model.pkl` fitxategian
gordetzen da, hurrengo fasean `scoring-api` zerbitzuak erabiltzeko.

## 1. Datu-karga eta azterketa

Lehenik, entrenamendu-datuak kargatu eta datu-multzoaren egitura aztertu
da: zutabeak, datu motak, estatistika deskribatzaileak eta
helburu-aldagaiaren banaketa.

``` python
import pandas as pd

df = pd.read_csv("../data/ai4i2020_train.csv")

df.info()
display(df.describe())

print(df["machine_failure"].value_counts())
print(df["machine_failure"].value_counts(normalize=True) * 100)
```

**Ondorioa:** datu-multzoa ez dago modu orekatuan banatuta. Hutsegiteak
kasuen %3,4 inguru dira; gainerako %96,6 inguru, berriz, hutsegiterik
gabeko kasuak dira. Desoreka horren ondorioz, accuracy metrika bakarrik
erabiltzea ez da nahikoa ereduaren kalitatea ebaluatzeko.

## 2. Preprocessing eta klase-desoreka

### 2.1. Datuak garbitzea

Hutsegite-motak adierazten dituzten zutabeak kendu dira,
helburu-aldagaia aurreikusteko informazio gehigarririk eman ez dezaten.
Zutabe horiek erabiltzeak **data leakage** eragin dezake.

`timestamp` zutabea ez da `drop_cols` zerrendan sartu, entrenamenduaren
CSV fitxategian ez dagoelako.

``` python
drop_cols = [
    "machine_id",
    "failure_TWF",
    "failure_HDF",
    "failure_PWF",
    "failure_OSF",
    "failure_RNF",
]

df_clean = df.drop(columns=drop_cols)
```

`machine_id` identifikatzaile bat da, eta ez du berez makinaren
hutsegite-arriskua azaltzen duen ezaugarri fisikorik ematen.

### 2.2. Kategoria-aldagaiaren kodetzea

`machine_type` aldagaia kategoriakoa denez, zenbakizko adierazpen
bihurtu da, eredua entrenatu ahal izateko.

``` python
df_encoded = pd.get_dummies(
    df_clean,
    columns=["machine_type"],
    drop_first=True,
    dtype=int,
)
```

`drop_first=True` erabiliz, lehenengo kategoria erreferentzia gisa
erabiltzen da. AI4I datu-multzoan `H`, `L` eta `M` kategoriak badaude,
espero diren zutabeak `machine_type_L` eta `machine_type_M` dira; `H`
kategoria erreferentzia izango da.

### 2.3. Feature engineering

Sentsoreen datuetatik abiatuta, hiru ezaugarri eratorri sortu dira.

``` python
# Potentziaren estimazioa
df_encoded["power"] = (
    df_encoded["rpm"] * df_encoded["torque_Nm"]
)

# Prozesuko eta giroko tenperaturen arteko diferentzia
df_encoded["temp_diff"] = (
    df_encoded["process_temp_K"] - df_encoded["air_temp_K"]
)

# Momentuaren eta erremintaren higaduraren arteko biderketa
df_encoded["torque_wear"] = (
    df_encoded["torque_Nm"] * df_encoded["tool_wear_min"]
)
```

| Ezaugarria | Azalpena |
|---|---|
| `power` | Biraketa-abiaduraren eta momentuaren arteko biderketa. |
| `temp_diff` | Prozesuko tenperaturaren eta giro-tenperaturaren arteko diferentzia. |
| `torque_wear` | Momentuaren eta erremintaren higaduraren arteko biderketa. |

Ezaugarri horien helburua da ereduari sentsoreen balio indibidualez gain
haien arteko erlazioak ere eskaintzea.

### 2.4. Klase-desoreka tratatzea

Datu-multzoan hutsegiteak gutxiengoa direnez, `class_weight="balanced"`
parametroa erabili da. Parametro horrek klase bakoitzari pisu desberdina
esleitzen dio, klase bakoitzaren maiztasunaren arabera. Horrela, ereduak
hutsegite-kasuak ikasteko aukera izan dezake, klase nagusiari gehiegizko
lehentasuna eman gabe.

Ikuspegi hau aukeratu da, ez duelako datu artifizialik sortzea eskatzen
eta jatorrizko entrenamendu-datuak mantentzen dituelako. Hala ere,
parametro horrek ez du berez recall handia bermatzen; horregatik, eredua
atalase desberdinekin ere ebaluatu da.

## 3. Entrenamendu- eta balidazio-multzoak sortzea

Preprocessing-aren ondoren, ezaugarriak (`X`) eta helburu-aldagaia (`y`)
banandu dira. Ereduak hamar ezaugarri erabiltzen ditu: bost
sentsore-ezaugarri, hiru ezaugarri eratorri eta bi `machine_type`
adierazle.

``` python
FEATURES = [
    "air_temp_K",
    "process_temp_K",
    "rpm",
    "torque_Nm",
    "tool_wear_min",
    "power",
    "temp_diff",
    "torque_wear",
    "machine_type_L",
    "machine_type_M",
]

X = df_encoded.drop(columns=["machine_failure"])
y = df_encoded["machine_failure"]

# Ezaugarrien ordena esplizituki finkatu
X = X[FEATURES]
```

Datuak entrenamendu- eta balidazio-multzoetan banatu dira:

``` python
from sklearn.model_selection import train_test_split

X_train, X_val, y_train, y_val = train_test_split(
    X,
    y,
    test_size=0.20,
    random_state=42,
    stratify=y,
)
```

-   Datuen %80 entrenatzeko erabiltzen da.
-   Datuen %20 balidatzeko erabiltzen da.
-   `random_state=42` erabiliz, banaketa errepikagarria da.
-   `stratify=y` erabiliz, klaseen proportzioa mantentzen da bi
    multzoetan.

**Egiaztapena:** `FEATURES` zerrendako hamar zutabeak `df_encoded`
datu-multzoan daudela egiaztatu behar da.
`pd.get_dummies(..., drop_first=True)` erabili denez, `machine_type_L`
eta `machine_type_M` zutabeak sortu direla baieztatu behar da.

## 4. Ereduaren entrenamendua

Hutsegiteak aurreikusteko `RandomForestClassifier` algoritmoa erabili
da.

``` python
from sklearn.ensemble import RandomForestClassifier

model = RandomForestClassifier(
    n_estimators=2000,
    max_depth=15,
    min_samples_split=4,
    class_weight="balanced",
    random_state=42,
)

model.fit(X_train, y_train)

print("Feature engineering-a erabiliz entrenatutako eredua.")
```

| Parametroa | Balioa | Azalpena |
|---|---:|---|
| `n_estimators` | 2000 | Ausazko basoa osatzen duten erabakien zuhaitzen kopurua. |
| `max_depth` | 15 | Zuhaitzen gehieneko sakonera mugatzen du. |
| `min_samples_split` | 4 | Nodo bat zatitzeko behar den gutxieneko lagin-kopurua. |
| `class_weight` | `balanced` | Klaseen desoreka kontuan hartzen du. |
| `random_state` | 42 | Entrenamenduaren emaitza errepikagarriagoa izatea ahalbidetzen du. |

2.000 zuhaitz erabiltzeak eredu konplexua entrenatzea ahalbidetzen du,
baina entrenamendu-denbora handitu dezake. `max_depth` eta
`min_samples_split` parametroek ereduaren konplexutasuna kontrolatzen
laguntzen dute.

## 5. Ereduaren ebaluazioa

Eredua balidazio-multzoarekin ebaluatu da. Horretarako,
nahasmen-matrizea (*confusion matrix*) eta sailkapen-txostena
(*classification report*) erabili dira.

### 5.1. Nahasmen-matrizea

``` python
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt

y_pred = model.predict(X_val)

cm = confusion_matrix(y_val, y_pred)

disp = ConfusionMatrixDisplay(
    confusion_matrix=cm,
    display_labels=["Hutsegiterik ez", "Hutsegitea"],
)

disp.plot()
plt.title("Nahasmen-matrizea")
plt.show()
```

| Terminoa | Esanahia | Enpresarentzako eragina |
|---|---|---|
| True Negative (TN) | Hutsegiterik ez dagoela zuzen aurreikusi da. | Ez da alferrikako alertarik sortzen. |
| False Positive (FP) | Hutsegitea iragarri da, baina ez da gertatzen. | Beharrezkoa ez den mantentze-lana eragin dezake. |
| False Negative (FN) | Hutsegiterik ez dagoela iragarri da, baina hutsegitea gertatzen da. | Hutsegite bat garaiz ez detektatzeko arriskua dago. |
| True Positive (TP) | Hutsegitea zuzen aurreikusi da. | Mantentze-lana aldez aurretik planifika daiteke. |

Proiektu honetan, bereziki garrantzitsua da **False Negative** kopurua
murriztea. Hutsegite bat garaiz ez detektatzeak makina gelditzea eta
ekoizpen-galerak eragin ditzake.

### 5.2. Sailkapen-txostena

``` python
from sklearn.metrics import classification_report

print(classification_report(y_val, y_pred))
```

-   **Precision:** hutsegite gisa sailkatutako kasuetatik zenbat diren
    benetako hutsegiteak.
-   **Recall:** benetako hutsegite guztietatik zenbat detektatzen dituen
    ereduak.
-   **F1-score:** precision eta recall metrikak bateratzen dituen
    neurria.
-   **Accuracy:** iragarpen guztietatik zenbat diren zuzenak.

Hutsegiteen detekzioan recall bereziki garrantzitsua da, benetako
hutsegiteak ez galtzea bilatzen baita. Hala ere, precision ere kontuan
hartu behar da, alerta faltsu gehiegik beharrezkoak ez diren ikuskapenak
eragin baititzakete.

### 5.3. Atalasearen doikuntza

Ereduak probabilitateak kalkulatzen ditu, eta atalase baten arabera
erabakitzen du kasua hutsegite gisa sailkatu behar den ala ez. Atalase
lehenetsia `0.5` da. Atalasea `0.4`ra jaitsiz gero, eredua hutsegiteak
detektatzeko sentikorragoa izan daiteke.

0,4ko atalasearekin lortutako sailkapen-txostena:

``` text
--- Sailkapen-txosten optimizatua (Atalasea 0.4) ---

              precision    recall  f1-score   support

           0       0.99      0.99      0.99      1546
           1       0.79      0.81      0.80        54

    accuracy                           0.99      1600
   macro avg       0.89      0.90      0.90      1600
weighted avg       0.99      0.99      0.99      1600
```

**Emaitzen interpretazioa:**

-   **Recall = 0,81:** benetako hutsegiteen %81 inguru detektatzen ditu
    ereduak.
-   **Precision = 0,79:** hutsegite gisa iragarritako kasuen %79 inguru
    benetako hutsegiteak dira.
-   **F1-score = 0,80:** precision eta recall balioak nahiko orekatuak
    dira hutsegiteen klasean.
-   **Accuracy = 0,99:** iragarpenen %99 inguru zuzenak dira, baina
    metrika hau ez da nahikoa ereduaren kalitatea ebaluatzeko,
    hutsegiteak oso gutxi direlako.

Precision eta recall balioak biribilduak direnez, ezin dira horietatik
nahasmen-matrizeko balio zehatzak ondorioztatu.

**Ondorioa:** 0,4ko atalasearekin, ereduak hutsegiteen detekzioan recall
ona lortzen du, eta precision ere nahiko altua da. Hala ere, oraindik
badago detektatzen ez den hutsegiteen ehuneko bat. Atalase hau mantendu
aurretik, beste atalase batzuetako emaitzak alderatzea komeni da.

## 6. Ezaugarrien garrantzia

`RandomForestClassifier` ereduaren `feature_importances_` atributuak
ezaugarri bakoitzak ereduan duen garrantzi erlatiboa aztertzeko aukera
ematen du.

``` python
import pandas as pd

feature_importance = pd.DataFrame({
    "Ezaugarria": X_train.columns,
    "Garrantzia": model.feature_importances_,
})

feature_importance = feature_importance.sort_values(
    by="Garrantzia",
    ascending=False,
)

display(feature_importance)
```

Grafiko bat sortzeko:

``` python
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))

plt.barh(
    feature_importance["Ezaugarria"],
    feature_importance["Garrantzia"],
)

plt.xlabel("Garrantzia")
plt.ylabel("Ezaugarria")
plt.title("Ereduaren ezaugarrien garrantzia")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()
```

Grafikoak ereduak zein ezaugarri erabiltzen dituen gehien erakusten du.
`tool_wear_min`, `torque_Nm` edo horiekin sortutako ezaugarriak
garrantzitsuak izatea espero daiteke, baina ezin da ezaugarririk
garrantzitsuena aurrez baieztatu: benetako emaitzak aztertu behar dira.

Garrantzi handia izateak ez du berez esan nahi ezaugarri horrek
hutsegitea eragiten duenik.

## 7. Eredua gordetzea

Entrenatutako eredua `.pkl` fitxategian gorde da, hurrengo fasean
`scoring-api` zerbitzuak erabili ahal izan dezan.

``` python
import joblib

joblib.dump(
    model,
    "../models/cnc_failure_model.pkl",
)

print("Eredua gordeta!")
```

Fitxategiaren bidea:

``` text
models/cnc_failure_model.pkl
```

**Garrantzitsua:** ereduak entrenamenduan erabilitako ezaugarri berak
eta ordena bera espero ditu. Horregatik, APIak ere `power`, `temp_diff`,
`torque_wear`, `machine_type_L` eta `machine_type_M` ezaugarriak sortu
behar ditu.

## 8. Scoring API egiaztatzea

Eredua gorde ondoren, APIa berrabiarazi eta egoera egiaztatu behar da.

Proiektuaren erro-karpetatik:

``` bash
docker compose restart scoring-api
```

Ondoren:

``` bash
curl http://localhost:8000/health
```

Windows PowerShell-en, erabili:

``` powershell
curl.exe http://localhost:8000/health
```

Espero den erantzuna:

``` json
{
  "status": "ok",
  "model_loaded": true,
  "model_path": "../models/cnc_failure_model.pkl"
}
```

Egiaztatu beharreko balioa:

``` json
"model_loaded": true
```

Errorea agertzen bada, zerbitzuaren logak kontsulta daitezke:

``` bash
docker compose logs --tail=100 scoring-api
```

APIaren egiaztapena entregagai gisa markatu aurretik, komandoa benetan
exekutatu eta erantzuna egiaztatu behar da.

------------------------------------------------------------------------

# 2. faseko entregagaien egiaztapen-zerrenda

-   [ ] `notebooks/02_training.ipynb` osatua eta dokumentatua.
-   [ ] Preprocessing urratsak eta haien arrazoiak azalduak.
-   [ ] Klase-desoreka tratatzeko `class_weight="balanced"` aukera
    justifikatua.
-   [ ] Nahasmen-matrizea sortua eta TN, FP, FN eta TP kontzeptuak
    azalduta.
-   [ ] Sailkapen-txostena eta metrikak interpretatuta.
-   [ ] Atalasearen doikuntza eta 0,4ko atalasearen emaitzak azalduak.
-   [ ] Ezaugarrien garrantzia aztertua.
-   [ ] `models/cnc_failure_model.pkl` fitxategia gordeta.
-   [ ] `/health` endpoint-a egiaztatuta eta `"model_loaded": true`
    erantzuna baieztatuta.
