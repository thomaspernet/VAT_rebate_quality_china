# Computation density

The density is computed using the following R code. The code is not clean since it has been done years ago. The data sources is available in Google Drive: https://drive.google.com/drive/folders/175DJv7ctuckR87WrW7ygqCkTeDr6WTjD?usp=sharing

## Step 1

```
code <- read_dta("/Users/Thomas/documents/Research/paper_2/data/density/codeprod_2002.dta") %>%
        lapply(FUN = one_entry) %>%
        data.frame() %>%
        mutate(temp = "balassa" ) %>%
        unite(name,temp,prods, sep ="", remove = FALSE) %>%
        distinct(hs6,name, .keep_all = TRUE) %>%
        select(1,3,4) %>%
        mutate(hs6 = as.character(hs6))

      complete <- code %>%
              select(1,3) %>%
              complete(hs6, prods) %>%
              left_join(code, by="hs6") %>%
              select(1,2,3) %>%
              left_join(code, by=c("prods.x" = "prods"))%>%
              filter(name.x != name.y)
              head(complete)
```

## Step 2

```
dat <- read_dta("/Users/Thomas/documents/Research/paper_2/data/additional_variable/density/base_balassa_reshape_2002.dta") %>%
      melt(id.vars = "codepays") %>%
      filter(value != 0) %>%
      mutate(variable = as.character(variable), codepays = as.numeric(codepays))
```

## Step 3

```
### compute rca
nb_rca <- dat %>%
  group_by(variable) %>%
  summarise(nb_i = sum(value))

head(pair)
### compute RCA identical and prox_i_j
        pair <- dat %>%
                right_join(dat, by="codepays",suffix = c(".hs1", ".hs2")) %>%
                select(1:5) %>%
                arrange(variable.hs2, variable.hs1) %>%
                filter(variable.hs1 != variable.hs2) %>%
                group_by(variable.hs1, variable.hs2) %>%
                summarise(m_i = sum(value.hs1)) %>%
                left_join(nb_rca, by=c("variable.hs2" = "variable")) %>%
                left_join(nb_rca, by=c("variable.hs1" = "variable")) %>%
                mutate(p_i_j = (m_i/nb_i.x)) %>%
                mutate(p_j_i = (m_i/nb_i.y)) %>%
                group_by(variable.hs1, variable.hs2) %>%
                mutate(prox_i_j = min(p_i_j, p_j_i)) %>%
                select(c(1,2,8)) %>%
                full_join(complete, by= c("variable.hs1"= "name.x", "variable.hs2"= "name.y")) %>%
                arrange(variable.hs1 ,prods.x) %>%
                mutate(prox_i_j = ifelse(is.na(prox_i_j),0,prox_i_j))
                ungroup()%>%
                group_by(hs6.x) %>%
                mutate(down = sum(prox_i_j))

        head(pair)
        str(pair)
        write.dta(pair,"/Users/Thomas/documents/Research/paper_2/data/density/pair.dta")
```

## Step 4

```
##### RCA city
        Sys.setlocale("LC_CTYPE", "en_US.UTF-8")
        dat <- read_dta("/Users/Thomas/documents/Research/paper_2/data/export_2002.dta") %>%
        lapply(FUN = one_entry) %>%
        data.frame() %>%
        mutate(hs6 = as.character(hs6), city = as.character(city))

        city <- read_dta("/Users/Thomas/documents/Research/paper_2/data/city_list/city_list_final.dta") %>%
        lapply(FUN = one_entry) %>%
        data.frame() %>%
          mutate(city = as.character(city))

        pair <- read_dta("/Users/Thomas/documents/Research/paper_2/data/density/pair.dta")
        attributes(pair$hs6.y) <- NULL


          dat <- dat %>%
          filter(exp_or_imp =="??????") %>%
          inner_join(city, by= "city") %>%
          group_by(geocode4_corr, hs6) %>%
          summarize(value =sum(value)) %>%
          ungroup()  %>%
            mutate(totalX = sum(value)) %>%
            group_by(geocode4_corr)%>%
            mutate(totalcity= sum(value)) %>%
            ungroup() %>%
            group_by(hs6)%>%
            mutate(product = sum(value)) %>%
            ungroup() %>%
            group_by(geocode4_corr, hs6) %>%
            mutate(city_product = sum(value)) %>%
            mutate(haut =city_product/totalcity, bas =  product/totalX, rca = haut/bas) %>%
            mutate(balassa = as.numeric(rca> 1)) %>%
            select(c(1, 2, 11))

            dat <- transform(dat,id=as.numeric(factor(geocode4_corr)))
            max(dat$id)

            id_city <- dat %>%
            select(1,4) %>%
            distinct()
            #260

          rca <- dat %>%
            filter(balassa==1) %>%
            left_join(code, by = "hs6") %>%
            select(2:6)

          datalist = list()

          for (i in 1:260) {
            final <- dat %>%
                filter(id ==i) %>%
                left_join(code, by = "hs6") %>%
                lapply(FUN = one_entry) %>%
                data.frame() %>%
                complete(hs6, prods) %>%
                mutate(id = ifelse(is.na(id),i,id)) %>%
                left_join(rca, by=c("prods", "id")) %>%
                mutate(balassa.x = ifelse(is.na(balassa.x),0,balassa.x),balassa.y = ifelse(is.na(balassa.y),0,balassa.y)) %>%
                filter(balassa.x != balassa.y) %>%
                mutate(hs6.x = as.character(hs6.x), hs6.y = as.character(hs6.y)) %>%
                left_join(pair,by= c("hs6.x", "hs6.y")) %>%
                select(c(1,5,7,12,14)) %>%
                group_by(hs6.x, id) %>%
                summarise(up_china_ville = sum(prox_i_j), down =mean(down), density_china_ville=up_china_ville/down) %>%
                select(1,2,5)
                datalist[[i]] <- final # add it to your list
          }

          big_data <- dplyr::bind_rows(datalist)

          big_data <- big_data %>%
            left_join(id_city, by= "id") %>%
            select(-2) %>%
            rename(hs6 = hs6.x) %>%
            mutate(geocode4_corr = as.character(geocode4_corr))

          head(big_data)
          tail(big_data)

          save(big_data,file="/Users/Thomas/documents/Research/paper_2/data/density/big_data.Rda")
          write.dta(big_data,"/Users/Thomas/documents/Research/paper_2/data/density/density.dta")
          load("/Users/Thomas/documents/Research/paper_2/data/density/big_data.Rda")
```
