---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.5.0
  kernelspec:
    display_name: SoS
    language: sos
    name: sos
---

<!-- #region kernel="SoS" -->
# New Baseline strategy data analysis

This notebook has been generated on 2020-07-12 

The objective of this notebook is to improve the baseline regression with a new strategy for the fixed effect

## Data source 

The data source of this dataset is:

- [vat_rebate_china.gz](https://console.cloud.google.com/storage/browser/_details/chinese_data/paper_project/vat_rebate_china.gz?project=valid-pagoda-132423)

### Variable name

The variables names and labels are the following:
<!-- #endregion -->

<!-- #region kernel="SoS" -->
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Variables</th>
      <th>Labels</th>
      <th>Types</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>int64_field_0</td>
      <td>int64_field_0</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>1</th>
      <td>hs6</td>
      <td>hs6</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>2</th>
      <td>eligible</td>
      <td>eligible</td>
      <td>bool</td>
    </tr>
    <tr>
      <th>3</th>
      <td>geocode4_corr</td>
      <td>geocode4_corr</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>4</th>
      <td>i</td>
      <td>i</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>5</th>
      <td>distance</td>
      <td>distance</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>6</th>
      <td>id_group</td>
      <td>id_group</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>7</th>
      <td>party_id</td>
      <td>party_id</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>8</th>
      <td>goods</td>
      <td>goods</td>
      <td>object</td>
    </tr>
    <tr>
      <th>9</th>
      <td>hs4</td>
      <td>hs4</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>10</th>
      <td>gbt</td>
      <td>gbt</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>11</th>
      <td>t_0</td>
      <td>t_0</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>12</th>
      <td>t</td>
      <td>t</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>13</th>
      <td>export_unit_price</td>
      <td>export_unit_price</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>14</th>
      <td>l_q</td>
      <td>l_q</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>15</th>
      <td>c</td>
      <td>c</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>16</th>
      <td>code</td>
      <td>code</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>17</th>
      <td>quality</td>
      <td>quality</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>18</th>
      <td>density_china_ville</td>
      <td>density_china_ville</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>19</th>
      <td>herf_com_s</td>
      <td>herf_com_s</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>20</th>
      <td>herf_com_o</td>
      <td>herf_com_o</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>21</th>
      <td>average</td>
      <td>average</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>22</th>
      <td>ln_import_tax</td>
      <td>ln_import_tax</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>23</th>
      <td>ln_vat_tax</td>
      <td>ln_vat_tax</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>24</th>
      <td>vat_m</td>
      <td>vat_m</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>25</th>
      <td>vat_reb_m</td>
      <td>vat_reb_m</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>26</th>
      <td>g1</td>
      <td>g1</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>27</th>
      <td>g2</td>
      <td>g2</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>28</th>
      <td>g3</td>
      <td>g3</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>29</th>
      <td>g4</td>
      <td>g4</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>30</th>
      <td>imported_variety</td>
      <td>imported_variety</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>31</th>
      <td>g5</td>
      <td>g5</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>32</th>
      <td>energy</td>
      <td>energy</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>33</th>
      <td>high_tech</td>
      <td>high_tech</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>34</th>
      <td>hi_s_e1</td>
      <td>hi_s_e1</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>35</th>
      <td>hi_s_us1</td>
      <td>hi_s_us1</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>36</th>
      <td>hi_s_e2</td>
      <td>hi_s_e2</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>37</th>
      <td>hi_s_us2</td>
      <td>hi_s_us2</td>
      <td>int64</td>
    </tr>
    <tr>
      <th>38</th>
      <td>rd_o</td>
      <td>rd_o</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>39</th>
      <td>rd_o_mean</td>
      <td>rd_o_mean</td>
      <td>float64</td>
    </tr>
    <tr>
      <th>40</th>
      <td>h_rd_o</td>
      <td>h_rd_o</td>
      <td>int64</td>
    </tr>
  </tbody>
</table>
<!-- #endregion -->

<!-- #region kernel="SoS" -->
# Analysis

Original code from [here](https://drive.google.com/open?id=1V6klFoNaPL30YdFOAonPaQs_v9Tn5rPq)

## Load the data

Please open links below to download the necessary data, and make sure to locate them in the same path as the notebook, otherwise, change the path.

- [vat_rebate_china.gz](https://console.cloud.google.com/storage/browser/_details/chinese_data/paper_project/vat_rebate_china.gz?project=valid-pagoda-132423)
- [baci92_2002.csv](https://console.cloud.google.com/storage/browser/_details/chinese_data/paper_project/intermediate_files/baci92_2002.csv?project=valid-pagoda-132423)
- [city_list_final.csv](https://console.cloud.google.com/storage/browser/_details/chinese_data/paper_project/intermediate_files/city_list_final.csv?project=valid-pagoda-132423)
- [export_2002.csv](https://console.cloud.google.com/storage/browser/_details/chinese_data/paper_project/intermediate_files/export_2002.csv?project=valid-pagoda-132423)

We decided to choose this way to load the data since it does not fit our current framework. The main data has been created using stata, which makes it very complicated to reproduce and visualize the pipeline. No documentation is available as well.
<!-- #endregion -->

```sos kernel="SoS"
from GoogleDrivePy.google_authorization import authorization_service
from GoogleDrivePy.google_platform import connect_cloud_platform

from pathlib import Path
import os

path = os.getcwd()
parent_path = str(Path(path).parent.parent)
project = 'valid-pagoda-132423'


auth = authorization_service.get_authorization(
    path_credential_gcp = "{}/creds/service.json".format(parent_path),
    verbose = False#
)

gcp_auth = auth.authorization_gcp()
gcp = connect_cloud_platform.connect_console(project = project, 
                                             service_account = gcp_auth) 

```

```sos kernel="SoS"
#query = (
#          "SELECT * "
#            "FROM China.vat_rebate_china "

#        )

#df_final = gcp.upload_data_from_bigquery(query = query, location = 'US')
#df_final.head()

```

```sos kernel="python3"
import function.latex_beautify as lb

%load_ext autoreload
%autoreload 2
```

```sos kernel="R"
options(warn=-1)
library(tidyverse)
library(lfe)
library(lazyeval)
library('progress')
path = "function/table_golatex.R"
source(path)
```

```sos kernel="R"

df_final <- read_csv('../temporary_local_data/vat_rebate_china.gz')

table(df_final$eligible)

```

<!-- #region kernel="SoS" -->
# New FE table 

To check how the FE are created, please refer to [here](https://drive.google.com/open?id=1pyMKTjtGJsVOorFniQDsFWR6b8DxDXYr)

```
egen g1 = group(id_group hs6 eligible)
*egen g1 = group(id_group hs6 )
egen g2 = group(i t_0 )
egen g3 = group(c t_0)
egen g4 = group(hs4 eligible t_0)
egen g5 = group(hs6 i t_0)
```
<!-- #endregion -->

<!-- #region kernel="SoS" -->
## Table 1: Baseline

Output latex table available here

- https://www.overleaf.com/project/5e6b52424e39670001dabbb0
    - table_1

![](https://drive.google.com/uc?export=view&id=1xrybwVOlhUL_76jB8lg0-CcrxsXA0hxk)
<!-- #endregion -->

```sos kernel="R"
#1    
t_1 <- felm(quality ~eligible:ln_vat_tax + imported_variety:t_0
            | g1+g2+g3+g4 |0 | id_group, df_final, exactDOF = TRUE)
#2
t_2 <-felm(quality ~ln_vat_tax+ imported_variety:t_0
             | g1+g2+g3+g4 |0 | id_group, df_final, eligible=="Yes", exactDOF = TRUE)
#3
t_3 <-felm(quality ~ln_vat_tax+ imported_variety:t_0
             | g1+g2+g3+g4 |0 | id_group, df_final, eligible=="No", exactDOF = TRUE)
#4
t_4 <-felm(quality ~eligible:ln_vat_tax + eligible:ln_import_tax+ imported_variety:t_0
             | g1+g2+g3+g4 |0 | id_group, df_final, exactDOF = TRUE)
#5
t_5 <-felm(quality ~eligible:ln_vat_tax + eligible:ln_import_tax +herf_com_o + imported_variety:t_0
             | g1+g2+g3+g4 |0 | id_group, df_final, exactDOF = TRUE)
```

```sos kernel="python3"
import os
try:
    os.remove("table_1.txt")
except:
    pass
try:
    os.remove("table_1.tex")
except:
    pass
```

```sos kernel="R"
dep <- "Dependent variable: Quality of firm $i$ for product $k$ in city $c$ at year $t$"
table_1 <- go_latex(list(
    t_1,
    t_2,
    t_3,
    t_4,
    t_5
),
    title="VAT export tax and firm's quality upgrading, baseline regression",
    dep_var = dep,
    addFE='',
    save=TRUE,
    note = FALSE,
    name="table_1.txt"
)
```

```sos kernel="python3"
tbe1 = "This table estimates eq(3). All estimation include firm-hs6, origin city-year," \
"destination-year, HS4-status-year fixed effect. Our regression adds initial firm size trend." \
"Note that $ordinary$ refers to the firms entitle to VAT refund, our treatment group." \
"Our control group is processing trade with supplied input, non eligible to VAT refund." \
"All firms belong to one group only, in consequence, the dummy $ordinary_{i}$ does not vary"  \
"within firm over time and is drop from the regression. $i$ stands for the firm,$k$ for" \
"the HS6 product, $t$ for the year and $s$ for the sector. Heteroskedasticity-robust standard" \
"errors clustered at the firm level appear in parentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."
```

```sos kernel="python3"
lb.beautify(table_number = 1,
            new_row= ['','', 'Eligible', 'No Eligible', ''],
           table_nte = tbe1,
           jupyter_preview = True,
            resolution = 150)
```

<!-- #region kernel="python3" -->
# Move tables
<!-- #endregion -->

```sos kernel="python3"
import glob, os, shutil
```

```sos kernel="python3"
list(
    map(
        lambda x: 
        shutil.move(x, os.path.join(os.getcwd(), 'Tables', x)),
        glob.glob('*.txt')
        
    )
)
```

```sos kernel="python3"
list(
    map(
        lambda x: 
        shutil.move(x, os.path.join(os.getcwd(), 'Tables', x)),
        glob.glob('*.tex')
        
    )
)
```

```sos kernel="python3"
list(
    map(
        os.remove,
        glob.glob('*.pdf')
        
    )
)
```

<!-- #region kernel="python3" -->
# Create Report
<!-- #endregion -->

```sos kernel="python3"
import os, time, shutil
from pathlib import Path

export = 'pdf' #'html'
export = 'html'

filename = '01_VAT_replicate_tables'
source_fn = filename + '.ipynb'
dest_fn = '{}.{}'.format(filename, export)
source_to_move = '{}.{}'.format(filename,export)
path = os.getcwd()
dest = os.path.join(path, 'Reports', dest_fn)
os.system('jupyter nbconvert --no-input --to {} {}'.format(export, source_fn))
shutil.move(source_to_move, dest)
```
