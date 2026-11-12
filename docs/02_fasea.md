# 2. Fasea — Preprocessing eta Entrenamendua (Offline)

## Helburua

Datu historikoetatik abiatuta, makinen hutsegiteak aurreikusteko eredu bat entrenatzea. Horretarako, datuak aztertu eta aurreprozesatu dira, ezaugarri berriak sortu dira eta hainbat algoritmo entrenatu eta ebaluatu dira.

Azken eredua `models/cnc_failure_model.pkl` fitxategian gordetzen da, hurrengo fasean `scoring-api` zerbitzuak erabili ahal izan dezan.

## 1. Datu-karga eta azterketa

Lehenik, entrenamendu-datuak kargatu dira, eta datu-multzoaren egitura aztertu da: zutabeak, datu motak, estatistika deskribatzaileak, balio faltak eta helburu-aldagaiaren banaketa.

```python
import pandas as pd

df = pd.read_csv("../data/ai4i2020_train.csv")

df.head()
df.info()
display(df.describe())

# Balio faltak aztertu
display(df.isnull().sum())

# Helburu-aldagaiaren banaketa
print(df["machine_failure"].value_counts())
print(df["machine_failure"].value_counts(normalize=True) * 100)
```

Datu-multzoak 8.000 erregistro eta 13 zutabe ditu. Azterketan ez da balio faltarik aurkitu.

**Helburu-aldagaiaren banaketa:**

- Hutsegiterik gabeko kasuak: %96,6 inguru.
- Hutsegite-kasuak: %3,4 inguru.

**Ondorioa:** datu-multzoa desorekatuta dago, hutsegiteak gutxiengo-klasea baitira. Horregatik, `accuracy` metrika bakarrik erabiltzea ez da nahikoa ereduaren kalitatea ebaluatzeko. Bereziki garrantzitsua da benetako hutsegiteak detektatzeko gaitasuna neurtzea.

### 1.1. Azterketa esploratzailea (EDA)

Datuen portaera hobeto ulertzeko, hainbat analisi grafiko egin dira:

- **Banaketa-histogramak:** sentsoreen balioen banaketa aztertzeko, hutsegiteak eta hutsegiterik gabeko kasuak bereizita.
- **Hutsegite-moten azterketa:** hutsegite mota bakoitzaren maiztasuna ikusteko.
- **Korrelazio-matrizea:** aldagai numerikoen arteko erlazioak eta helburu-aldagaiarekiko korrelazioak aztertzeko.
- **Pairplot grafikoa:** sentsoreen arteko erlazioak eta klaseen arteko desberdintasun posibleak ikusteko.
- **Boxplot grafikoak:** sentsoreen balio muturrekoak (*outliers*) identifikatzeko.
- **Makinen erregistro-kopurua:** makina bakoitzak zenbat erregistro dituen aztertzeko.

Analisi horien helburua datuen egitura eta portaera ulertzea da, aurreprozesatzean eta eredua aukeratzean erabaki egokiak hartu ahal izateko.

## 2. Aurreprozesatzea eta klase-desoreka

### 2.1. Datuak garbitzea

Hutsegite-motak adierazten dituzten zutabeak kendu dira, helburu-aldagaia aurreikusteko informazio gehigarririk eman ez dezaten. Zutabe horiek erabiltzeak **datu-ihesa (*data leakage*)** eragin dezake, benetako hutsegitearen ondoriozko informazioa ereduan sartuko litzatekeelako.

`machine_id` zutabea ere kendu da, identifikatzaile bat delako eta ez duelako berez makinaren hutsegite-arriskua azaltzen duen ezaugarri fisikorik ematen.

`timestamp` zutabea ez da ezabatu beharreko zutabeen zerrendan sartu, entrenamendurako erabilitako CSV fitxategian ez dagoelako.

```python
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

### 2.2. Kategoria-aldagaiaren kodetzea

`machine_type` aldagaia kategoriakoa denez, zenbakizko adierazpen bihurtu da, ereduek erabili ahal izan dezaten.

Horretarako, One-Hot Encoding teknika erabili da:

```python
df_encoded = pd.get_dummies(
    df_clean,
    columns=["machine_type"],
    drop_first=True,
    dtype=int,
)
```

`drop_first=True` erabiliz, lehenengo kategoria erreferentzia gisa erabiltzen da. AI4I datu-multzoan `H`, `L` eta `M` kategoriak daudenez, sortutako zutabeak `machine_type_L` eta `machine_type_M` dira. `H` kategoria erreferentzia gisa erabiltzen da.

### 2.3. Ezaugarri berriak sortzea (Feature Engineering)

Sentsoreen datuetatik abiatuta, hiru ezaugarri eratorri sortu dira. Helburua da ereduari sentsoreen balio indibidualez gain haien arteko erlazioak ere eskaintzea.

```python
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

Ezaugarri eratorri horiek makinen funtzionamenduaren inguruko informazio gehigarria ematen diote ereduari.

### 2.4. Klase-desoreka tratatzea

Datu-multzoan hutsegiteak gutxiengoa direnez, `RandomForestClassifier` ereduan `class_weight="balanced"` parametroa erabili da.

Parametro horrek klase bakoitzari pisu desberdina esleitzen dio, klase bakoitzaren maiztasunaren arabera. Horrela, ereduak hutsegite-kasuak ikasteko aukera handiagoa izan dezake, klase nagusiari gehiegizko lehentasuna eman gabe.

Ikuspegi hori aukeratu da, ez duelako datu artifizialik sortzea eskatzen eta jatorrizko entrenamendu-datuak mantentzen dituelako. Hala ere, parametro horrek ez du berez `recall` handia bermatzen; horregatik, beste estrategia batzuk ere aztertu dira, hala nola SMOTE.

## 3. Entrenamendu- eta balidazio-multzoak sortzea

Aurreprozesatzea amaitu ondoren, ezaugarriak (`X`) eta helburu-aldagaia (`y`) banandu dira.

Ereduak hamar ezaugarri erabiltzen ditu:

- Bost sentsore-ezaugarri.
- Hiru ezaugarri eratorri.
- Bi `machine_type` adierazle.

Erabilitako ezaugarrien ordena esplizituki finkatu da:

```python
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

X = df_encoded[FEATURES]
y = df_encoded["machine_failure"]
```

Datuak entrenamendu- eta balidazio-multzoetan banatu dira:

```python
from sklearn.model_selection import train_test_split

X_train, X_val, y_train, y_val = train_test_split(
    X,
    y,
    test_size=0.20,
    random_state=42,
    stratify=y,
)
```

Banaketa honela egin da:

- Datuen %80 entrenatzeko erabiltzen da.
- Datuen %20 balidatzeko erabiltzen da.
- `random_state=42` erabiliz, banaketa errepikagarria da.
- `stratify=y` erabiliz, klaseen proportzioa mantentzen da bi multzoetan.

**Egiaztapena:** `FEATURES` zerrendako hamar zutabeak `df_encoded` datu-multzoan daudela egiaztatu da. `machine_type_L` eta `machine_type_M` zutabeak One-Hot Encoding prozesuaren bidez sortu dira.

## 4. Ereduen entrenamendua eta konparazioa

Hutsegiteak aurreikusteko hainbat estrategia aztertu dira, eredu egokiena aukeratzeko.

### 4.1. Random Forest

Lehenengo eredua `RandomForestClassifier` algoritmoaren bidez entrenatu da.

```python
from sklearn.ensemble import RandomForestClassifier

model = RandomForestClassifier(
    n_estimators=2000,
    max_depth=15,
    min_samples_split=4,
    class_weight="balanced",
    random_state=42,
)

model.fit(X_train, y_train)
```

| Parametroa | Balioa | Azalpena |
|---|---:|---|
| `n_estimators` | 2000 | Ausazko basoa osatzen duten erabakien zuhaitzen kopurua. |
| `max_depth` | 15 | Zuhaitzen gehieneko sakonera mugatzen du. |
| `min_samples_split` | 4 | Nodo bat zatitzeko behar den gutxieneko lagin-kopurua. |
| `class_weight` | `balanced` | Klaseen desoreka kontuan hartzen du. |
| `random_state` | 42 | Entrenamenduaren emaitza errepikagarriagoa izatea ahalbidetzen du. |

2.000 zuhaitz erabiltzeak eredu konplexua entrenatzea ahalbidetzen du, baina entrenamendu-denbora handitu dezake. `max_depth` eta `min_samples_split` parametroek ereduaren konplexutasuna kontrolatzen laguntzen dute.

### 4.2. Stratified K-Fold Cross-Validation

Ereduaren egonkortasuna eta orokortzeko gaitasuna aztertzeko, Stratified K-Fold balidazio gurutzatua erabili da.

Teknika horrek entrenamendu-multzoa bost zatitan banatzen du. Iterazio bakoitzean, lau zati entrenatzeko eta bat balidatzeko erabiltzen dira. Gainera, klaseen proportzioa mantentzen da zati bakoitzean.

```python
from sklearn.model_selection import StratifiedKFold, cross_validate

skf = StratifiedKFold(
    n_splits=5,
    shuffle=True,
    random_state=42,
)

scoring = {
    "recall": "recall",
    "roc_auc": "roc_auc",
    "precision": "precision",
    "f1": "f1",
}

cv_results = cross_validate(
    model,
    X_train,
    y_train,
    cv=skf,
    scoring=scoring,
    n_jobs=-1,
)
```

Emaitzen batezbestekoa eta desbideratze estandarra kalkulatu dira, metrika bakoitzaren portaera aztertzeko.

| Metrika | Batezbestekoa | Desbideratze estandarra |
|---|---:|---:|
| Recall | 0,8429 | 0,0614 |
| ROC-AUC | 0,9797 | 0,0156 |
| Precision | 0,8503 | 0,0547 |
| F1-score | 0,8432 | 0,0215 |

Emaitzek erakusten dute Random Forest ereduak emaitza onak lortzen dituela entrenamendu-multzoaren barruko balidazio gurutzatuan. Bost iterazioetan lortutako emaitzen batezbestekoa eta desbideratze estandarra erabiliz, ereduaren portaera aztertu da.

### 4.3. XGBoost eredua

Bigarren algoritmo gisa, XGBoost aztertu da, *boosting* teknika erabiltzen duen sailkatzailea.

Klase-desoreka kontuan hartzeko, `scale_pos_weight` parametroa erabili da. Balio hori klase negatiboen eta positiboen arteko proportzioaren arabera kalkulatu da.

```python
import xgboost as xgb

scale_weight = (
    (y_train == 0).sum() / (y_train == 1).sum()
)

xgb_model = xgb.XGBClassifier(
    n_estimators=100,
    learning_rate=0.1,
    max_depth=5,
    scale_pos_weight=scale_weight,
    random_state=42,
    eval_metric="logloss",
)

xgb_model.fit(X_train, y_train)

y_pred_xgb = xgb_model.predict(X_val)
y_proba_xgb = xgb_model.predict_proba(X_val)[:, 1]

print(classification_report(y_val, y_pred_xgb))
print(roc_auc_score(y_val, y_proba_xgb))
```

Atal honetan, XGBoost ereduaren hasierako ebaluazioa egin da, algoritmoaren sailkapen-irizpide lehenetsia erabiliz. Ereduen arteko konparazioan, berriz, 0,40ko atalase bera aplikatu da, emaitzak baldintza berdinetan alderatzeko.

### 4.4. Random Forest + SMOTE

Klase-desoreka tratatzeko beste aukera bat SMOTE teknika erabiltzea da.

SMOTEk gutxiengo-klaseko lagin sintetikoak sortzen ditu, klaseen arteko desoreka murrizteko.

Horretarako, `imblearn.pipeline.Pipeline` erabili da. Horrela, SMOTE entrenamendu-zatietan bakarrik aplikatzen da balidazio gurutzatuan, eta balidazio-datuak ez dira lagin sintetikoak sortzeko erabiltzen.

```python
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_validate

smote_model = ImbPipeline([
    ("smote", SMOTE(random_state=42)),
    ("classifier", RandomForestClassifier(
        n_estimators=2000,
        max_depth=15,
        min_samples_split=4,
        class_weight=None,
        random_state=42,
    )),
])

smote_results = cross_validate(
    smote_model,
    X_train,
    y_train,
    cv=skf,
    scoring=scoring,
    n_jobs=-1,
)
```

SMOTEren bidez lortutako emaitzak ere aztertu dira, Random Forest ereduaren emaitzekin alderatzeko.

### 4.5. Ereduen konparazioa

Hiru estrategiak alderatu dira: Random Forest, XGBoost eta Random Forest + SMOTE.

Balidazio-multzoan alderaketa egiteko, SMOTE eredua entrenamendu-multzo osoarekin entrenatu behar da lehenik:

```python
smote_model.fit(X_train, y_train)
```

Urrats hori beharrezkoa da, `cross_validate()` funtzioak pipeline-aren kopiak entrenatzen dituelako, baina ez duelako jatorrizko `smote_model` objektua entrenatuta uzten.

Ondoren, eredu bakoitzaren hutsegite-probabilitateak kalkulatu eta 0,40ko atalase bera aplikatu da, konparazioa baldintza berdinetan egiteko.

```python
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)

atalasea = 0.40

ereduak = {
    "Random Forest": model,
    "XGBoost": xgb_model,
    "Random Forest + SMOTE": smote_model,
}

for izena, eredua in ereduak.items():
    probabilitateak = eredua.predict_proba(X_val)[:, 1]
    iragarpenak = (probabilitateak >= atalasea).astype(int)

    print(f"\n--- {izena} ---")
    print(f"Precision: {precision_score(y_val, iragarpenak):.4f}")
    print(f"Recall: {recall_score(y_val, iragarpenak):.4f}")
    print(f"F1-score: {f1_score(y_val, iragarpenak):.4f}")
    print(f"ROC-AUC: {roc_auc_score(y_val, probabilitateak):.4f}")
```

Lortutako emaitzak:

| Eredua | Precision | Recall | F1-score | ROC-AUC |
|---|---:|---:|---:|---:|
| Random Forest | 0,8148 | 0,8148 | 0,8148 | 0,9792 |
| XGBoost | 0,5946 | 0,8148 | 0,6875 | 0,9682 |
| Random Forest + SMOTE | 0,6027 | 0,8148 | 0,6929 | 0,9696 |

**Ondorioa:** egindako konparazioan, Random Forest ereduak emaitza orekatuenak lortu ditu. Precision eta F1-score balio altuagoak ditu, eta ROC-AUC ere handiena da. Horregatik, azken eredutzat Random Forest aukeratu da.

## 5. Ereduaren ebaluazioa

Aukeratutako Random Forest eredua balidazio-multzoarekin ebaluatu da. Horretarako, nahasmen-matrizea (*confusion matrix*), sailkapen-txostena (*classification report*) eta ROC-AUC metrika erabili dira.

### 5.1. Hasierako ebaluazioa: 0,35eko atalasea

Lehenengo ebaluazioan, hutsegite-probabilitateak kalkulatu eta 0,35eko atalasea aplikatu da.

```python
y_prob = model.predict_proba(X_val)[:, 1]

atalasea_baseline = 0.35

y_pred = (
    y_prob >= atalasea_baseline
).astype(int)
```

Atalase hori erabiliz, nahasmen-matrizea eta sailkapen-txostena kalkulatu dira:

```python
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt

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
| False Positive (FP) | Hutsegitea iragarri da, baina ez da gertatzen. | Beharrezkoa ez den mantentze-lana edo ikuskapena eragin dezake. |
| False Negative (FN) | Hutsegiterik ez dagoela iragarri da, baina hutsegitea gertatzen da. | Hutsegite bat garaiz ez detektatzeko arriskua dago. |
| True Positive (TP) | Hutsegitea zuzen aurreikusi da. | Mantentze-lana aldez aurretik planifika daiteke. |

Proiektu honetan, bereziki garrantzitsua da **False Negative** kopurua murriztea. Hutsegite bat garaiz ez detektatzeak makina gelditzea eta ekoizpen-galerak eragin ditzake.

### 5.2. Sailkapen-txostena

```python
from sklearn.metrics import classification_report

print(classification_report(y_val, y_pred))
```

Erabilitako metrika nagusiak:

- **Precision (doitasuna):** hutsegite gisa sailkatutako kasuetatik zenbat diren benetako hutsegiteak.
- **Recall (sentikortasuna):** benetako hutsegite guztietatik zenbat detektatzen dituen ereduak.
- **F1-score:** precision eta recall metrikak bateratzen dituen neurria.
- **Accuracy (asmatze-tasa):** iragarpen guztietatik zenbat diren zuzenak.

Hutsegiteen detekzioan recall bereziki garrantzitsua da, benetako hutsegiteak ez galtzea bilatzen baita. Hala ere, precision ere kontuan hartu behar da, alerta faltsu gehiegik beharrezkoak ez diren ikuskapenak eragin baititzakete.

### 5.3. Atalasearen doikuntza

Ereduak hutsegite-probabilitate bat kalkulatzen du, eta atalase baten arabera erabakitzen du kasua hutsegite gisa sailkatu behar den ala ez.

Atalase lehenetsia 0,50 da. Atalase hori jaistean, hutsegite gehiago detektatzeko aukera dago, baina alarma faltsu gehiago ere sor daitezke. Atalasea igotzean, berriz, alarma faltsuak murriztu daitezke, baina benetako hutsegite batzuk detektatu gabe gera daitezke.

Horregatik, hainbat atalase aztertu dira, Precision-Recall kurba eta ebaluazio-metrikak erabiliz. Azken atalase operatibo gisa 0,40 aukeratu da.

#### Kostuen estimazioa

Atalase bakoitzaren eragin ekonomikoa kalkulatzeko, foroan emandako kasu praktikoaren batez besteko kostuak erabili dira:

- **FN (False Negative):** benetako hutsegite bat detektatzen ez denean gertatzen da. Makina ekoizpenean dagoela huts egin dezake, eta horrek ekoizpen-linearen geldialdia eta premiazko konponketak eragin ditzake. Batez besteko kostua: **5.000 €**.
- **FP (False Positive):** makinak arazorik ez duen arren, ereduak hutsegitea dagoela iragartzen du. Horrek alferrikako ikuskapena eta ekoizpenaren eten laburra eragin ditzake. Batez besteko kostua: **200 €**.
- **TP (True Positive):** benetako hutsegite bat garaiz detektatzen da, eta mantentze prebentiboa egin daiteke. Piezen kostua kontuan hartuta, batez besteko kostua: **300 €**.
- **TN (True Negative):** makinak behar bezala funtzionatzen du eta ereduak egoera hori zuzen sailkatzen du. Ez du kostu gehigarririk eragiten: **0 €**.

Kostu horiek erabiliz, atalase bakoitzaren kostu ekonomiko estimatua honako formula honen bidez kalkulatu da:

`Kostu osoa = (FN × 5.000 €) + (FP × 200 €) + (TP × 300 €) + (TN × 0 €)`

#### Atalaseen arteko konparazioa

| Atalasea | Recall | Precision | F1-score | FP | FN | TP | Kostu osoa (€) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0,20 | 0,852 | 0,438 | 0,579 | 59 | 8 | 46 | 65.600 |
| 0,30 | 0,833 | 0,634 | 0,720 | 26 | 9 | 45 | 63.700 |
| 0,35 | 0,815 | 0,721 | 0,765 | 17 | 10 | 44 | 66.600 |
| **0,40** | **0,815** | **0,815** | **0,815** | **10** | **10** | **44** | **65.200** |
| 0,50 | 0,796 | 0,878 | 0,835 | 6 | 11 | 43 | 69.100 |

#### Emaitzen interpretazioa

- **Recall:** benetako hutsegiteen zer ehuneko detektatzen duen adierazten du. 0,30eko atalasearekin, ereduak benetako hutsegiteen %83,3 detektatzen ditu.
- **Precision:** hutsegite gisa iragarritako kasuetatik zenbat diren benetako hutsegiteak adierazten du. 0,40ko atalaseak %81,5eko precision lortzen du.
- **F1-score:** precision eta recall metrikak bateratzen dituen neurria da. 0,50eko atalaseak baliorik handiena lortzen du, 0,835ekoarekin.
- **Accuracy:** ez da erabaki nagusirako nahikoa, hutsegiteak datu-multzoan gutxiengoa direlako.

Emaitzek erakusten dute atalasea jaisteak hutsegite gehiago detektatzeko aukera ematen duela, baina alarma faltsuen kopurua ere handitzen dela. Alderantziz, atalasea igotzean alarma faltsu gutxiago sortzen dira, baina benetako hutsegite gehiago detektatu gabe gera daitezke.

#### Kostu ekonomikoaren azterketa

Kostuen arabera, 0,30eko atalaseak emaitzarik onena lortzen du, **63.700 €-ko kostu ekonomiko estimatuarekin**. 0,40ko atalaseak, berriz, 65.200 €-ko kostua du, eta 0,50ekoak 69.100 €-koa.

0,30eko eta 0,40ko atalaseak alderatuz, lehenengoak positibo faltsu gehiago sortzen ditu (26, 10en ordez), baina negatibo faltsu bat gutxiago (9, 10en ordez). Horren ondorioz, 0,30eko atalaseak kostu ekonomiko txikiagoa eragiten du, nahiz eta alarma faltsu gehiago sortu.

Bestalde, 0,50eko atalaseak F1-score altuena lortzen du, baina negatibo faltsu gehiago sortzen ditu (11), eta horrek kostu ekonomikoa handitzen du. Horrek erakusten du F1-score altuena lortzea ez dela beti aukerarik egokiena industria-ingurune batean, errore mota bakoitzak ondorio ekonomiko desberdinak dituelako.

#### Ondorioa: azken atalase operatiboa

Kostu ekonomiko txikiena 0,30eko atalasearekin lortzen bada ere, proiektu honetan **0,40ko atalasea hautatu da azken atalase operatibo gisa**.

Erabaki hori hartzeko, ez da kostu ekonomikoa bakarrik kontuan hartu, baizik eta hutsegiteak detektatzeko gaitasuna eta alarma faltsuek eragindako ondorio operatiboak ere aztertu dira.

0,30eko atalaseak 63.700 €-ko kostu estimatua du, eta 0,40koak, berriz, 65.200 €-koa. Beraz, bien arteko aldea 1.500 €-koa da balidazio-multzoan. Hala ere, 0,40ko atalaseak positibo faltsuen kopurua 26tik 10era murrizten du; hau da, beharrezkoak ez diren 16 ikuskapen saihesten ditu balidazio-multzoan. Gainera, precision-a 0,634tik 0,815era igotzen du, eta F1-score-a ere hobetzen du, 0,720tik 0,815era.

Bestalde, recall-a 0,833tik 0,815era jaisten da; beraz, 0,30eko atalaseak hutsegite gehiago detektatzen ditu. Hala ere, 0,40ko atalaseak benetako hutsegiteen %81,5 detektatzen ditu, eta emaitza hori egokitzat jo da proiektuaren helburuetarako.

Horrenbestez, **0,40ko atalasea aukeratu da detekzio-gaitasunaren, alarma faltsuen kopuruaren eta mantentze-lanen eraginkortasunaren arteko oreka bilatzeko**. Aukeratutako atalaseak ez du kostu ekonomiko estimatu txikiena lortzen, baina positibo faltsuak nabarmen murrizten ditu eta iragarpen fidagarriagoak eskaintzen ditu.

## 6. Ezaugarrien garrantzia

`RandomForestClassifier` ereduaren `feature_importances_` atributuak ezaugarri bakoitzak ereduan duen garrantzi erlatiboa aztertzeko aukera ematen du.

Azterketa hori egiteko, entrenatutako ereduaren garrantzi-balioak taula batean bildu eta handienetik txikienera ordenatu dira.

```python
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

Grafikoa sortzeko:

```python
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

Grafikoak ereduak zein ezaugarri erabiltzen dituen gehien erakusten du. `tool_wear_min`, `torque_Nm` edo horiekin sortutako ezaugarriak garrantzitsuak izan daitezke, baina benetako emaitzak aztertu behar dira.

**Garrantzitsua:** garrantzi handia izateak ez du berez esan nahi ezaugarri horrek hutsegitea eragiten duenik. Ezaugarrien garrantzia ereduak egindako banaketetan oinarritzen da, eta ez du kausalitatea frogatzen.

## 7. Eredua gordetzea

Aurreko ebaluazioaren ondoren, hautatutako Random Forest eredua `.pkl` fitxategian gorde da, hurrengo fasean `scoring-api` zerbitzuak erabili ahal izan dezan.

```python
import os
import joblib

os.makedirs("../models", exist_ok=True)

joblib.dump(
    model,
    "../models/cnc_failure_model.pkl",
)

print("Eredua gordeta!")
```

Fitxategiaren bidea:

```text
models/cnc_failure_model.pkl
```

Gordetako eredua berriro kargatu eta egiaztatu daiteke:

```python
loaded = joblib.load("../models/cnc_failure_model.pkl")
```

**Garrantzitsua:** ereduak entrenamenduan erabilitako hamar ezaugarri berak eta ordena bera espero ditu. Horregatik, APIak ere `power`, `temp_diff`, `torque_wear`, `machine_type_L` eta `machine_type_M` ezaugarriak sortu behar ditu, eta gainerako sentsore-ezaugarriak dagokien ordenan eman.

## 8. Scoring APIaren integrazioa eta egiaztapena

Atal honetan, 2. fasean entrenatutako eredua `scoring-api` zerbitzuan erabil daitekeela egiaztatzen da. Horretarako, APIaren osasuna eta predikzio-endpointaren funtzionamendua probatzen dira.

`scoring-api` FastAPI mikrozerbitzu bat da. NiFi-k `InvokeHTTP` bidez deitzen dio, sentsoreen irakurketak bidaltzeko eta ereduaren bidez hutsegite-probabilitatea jasotzeko.

### 8.1. Eredua kargatzea

APIak abiaraztean behin kargatzen du entrenatutako eredua, `lifespan` mekanismoaren bidez.

Ereduaren bidea `MODEL_PATH` ingurune-aldagaiaren bidez konfigura daiteke. Aldagai hori definituta ez badago, honako bide hau erabiltzen da:

```python
MODEL_PATH = os.getenv(
    "MODEL_PATH",
    "../models/cnc_failure_model.pkl",
)
```

Eredua behar bezala kargatzen bada, APIak eskaera guztietan erabil dezake, fitxategia behin eta berriro kargatu beharrik gabe.

### 8.2. APIaren autentifikazioa

APIak `X-API-Key` goiburua erabiltzen du sarbidea kontrolatzeko.

Gako sekretua `API_KEY_SECRET` ingurune-aldagaiaren bidez konfiguratzen da:

```python
API_KEY_SECRET = os.getenv(
    "API_KEY_SECRET",
    "super-secret-key-change-in-prod",
)
```

`/health` eta `/predict` endpointetara sartzeko, eskaerak API gakoa bidali behar du. Gakoa falta bada edo zuzena ez bada, zerbitzuak `403 Forbidden` erantzuna itzultzen du.

### 8.3. Sarrerako datuen balidazioa

`/predict` endpointak `SentsoreSaria` Pydantic eredua erabiltzen du jasotako datuak balidatzeko.

Eskaerak honako eremu hauek izan behar ditu:

| Eremua | Balidazioa |
|---|---|
| `timestamp` | Gehienez 50 karaktere. |
| `machine_id` | `CNC-` aurrizkia eta hiru digitu. |
| `machine_type` | `L`, `M` edo `H`. |
| `air_temp_K` | 200 eta 400 K artean. |
| `process_temp_K` | 200 eta 400 K artean. |
| `rpm` | 0 eta 10.000 artean. |
| `torque_Nm` | 0 eta 500 Nm artean. |
| `tool_wear_min` | 0 eta 1.000 minutu artean. |

Balio horiek zehaztutako baldintzak betetzen ez badituzte, eskaera ez da predikzio-prozesura pasatzen.

### 8.4. Ezaugarri berriak sortzea eta predikzioa

APIak jasotako sentsore-balioekin DataFrame bat sortzen du, eta entrenamenduan erabilitako hiru ezaugarri eratorriak kalkulatzen ditu:

- `power`: rpm × torque_Nm.
- `temp_diff`: process_temp_K − air_temp_K.
- `torque_wear`: torque_Nm × tool_wear_min.

Ondoren, `machine_type` aldagaia entrenamenduko kodifikazio berarekin bihurtzen du:

- `machine_type_L`: 1 izango da makina mota `L` bada; bestela, 0.
- `machine_type_M`: 1 izango da makina mota `M` bada; bestela, 0.
- `H` makina mota erreferentziazko kategoria da, eta bi adierazleak 0 izango ditu.

Azkenik, entrenamenduan erabilitako ezaugarrien ordena mantentzen da, ereduari datuak formatu egokian bidaltzeko.

Ereduak `predict_proba()` metodoaren bidez hutsegite-probabilitatea kalkulatzen du. Horretarako, hutsegiteari dagokion klasearen probabilitatea hartzen da.

### 8.5. Atalasea eta alerta-mailak

APIak 2. fasean hautatutako **0,40ko atalase operatiboa** erabiltzen du.

Hutsegite-probabilitatearen arabera, hiru alerta-maila definitu dira:

| Probabilitatea | Alerta-maila | Esanahia |
|---|---|---|
| 0,40 baino txikiagoa | `OK` | Ez da hutsegite-arrisku nahikorik identifikatu. |
| 0,40 eta 0,80 artean (biak barne) | `KONTUZ` | Hutsegite-arriskua dago; arreta eta jarraipena behar dira. |
| 0,80 baino handiagoa | `LARRIA` | Hutsegite-probabilitate handia dago; esku-hartzea aztertzea komeni da. |

`failure_predicted` eremua `True` izango da probabilitatea 0,40 edo handiagoa denean; bestela, `False`.

APIak honako eremu hauek itzultzen ditu:

| Eremua | Azalpena |
|---|---|
| `machine_id` | Iragarpena zein makinari dagokion. |
| `failure_predicted` | Hutsegitea aurreikusi den ala ez. |
| `failure_probability` | Hutsegitearen probabilitatea, lau hamartarrekin. |
| `alert_level` | Sortutako alerta-maila: `OK`, `KONTUZ` edo `LARRIA`. |

### 8.6. APIaren egoera egiaztatzea (`/health`)

Eredua gorde ondoren, APIa berrabiarazi behar da, azken eredua kargatu dezan.

Proiektuaren erro-karpetatik:

```bash
docker compose restart scoring-api
```

Ondoren, `/health` endpointa probatu behar da. Endpoint honek autentifikazioa behar duenez, API gakoa bidali behar da `X-API-Key` goiburuan.

Windows PowerShell-en:

```powershell
curl.exe -H "X-API-Key: <API_KEY_SECRET>" http://localhost:8000/health
```

`<API_KEY_SECRET>` tokian APIaren konfigurazioan ezarritako benetako gakoa jarri behar da.

Espero den erantzunaren adibidea:

```json
{
    "status": "ok",
    "model_loaded": true,
    "model_path": "../models/cnc_failure_model.pkl"
}
```

Egiaztatu beharreko balio nagusia:

```json
"model_loaded": true
```

Horrek adierazten du APIak eredua behar bezala kargatu duela.

### 8.7. Predikzio-endpointa probatzea (`/predict`)

`/predict` endpointak POST eskaera bat jasotzen du, sentsoreen datuekin, eta hutsegitearen predikzioa itzultzen du.

Adibidez, PowerShell-en:

```powershell
curl.exe -X POST http://localhost:8000/predict `
  -H "Content-Type: application/json" `
  -H "X-API-Key: <API_KEY_SECRET>" `
  -d '{
    "timestamp": "2026-04-29T10:57:50Z",
    "machine_id": "CNC-039",
    "machine_type": "L",
    "air_temp_K": 299.0,
    "process_temp_K": 308.6,
    "rpm": 1587,
    "torque_Nm": 38.6,
    "tool_wear_min": 124
  }'
```

Erantzunaren adibidea:

```json
{
    "machine_id": "CNC-039",
    "failure_predicted": false,
    "failure_probability": 0.1234,
    "alert_level": "OK"
}
```

Probabilitatea eta alerta-maila ereduak kalkulatzen ditu; beraz, benetako erantzuna bidalitako datuen eta kargatutako ereduaren araberakoa izango da.

### 8.8. Erroreen diagnostikoa

APIak eredua kargatzean edo predikzio bat egitean errorea badu, zerbitzuaren logak kontsulta daitezke:

```bash
docker compose logs --tail=100 scoring-api
```

APIaren egiaztapena entregagai gisa markatu aurretik, komandoak benetan exekutatu eta erantzunak egiaztatu behar dira.

**Fasearen azken egiaztapena:** entrenatutako eredua behar bezala gorde dela, APIak eredua kargatu duela eta `/predict` endpointak sentsoreen datuak jasotzen dituela egiaztatu behar da.
