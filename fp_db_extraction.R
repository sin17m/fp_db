library(tidyverse)
library(lubridate)
library(DBI)
library(RSQLite)

fp_db <- dbConnect(SQLite(), "fprime.fpdb")

fp_tbl <- db_list_tables(fp_db)
fp_tbl

n_att <- tbl(fp_db, "nodeAttribute") %>% 
  collect() %>% 
  select(nodeAttribute_id=id,
         att_name = name)

att <- tbl(fp_db, "attributeValue") %>% 
  collect() %>% 
  left_join(n_att) %>% 
  select(-nodeAttribute_id) %>% 
  spread(att_name, value)

natt <- dbReadTable(fp_db, "nodeAttribute")

trials <- tbl(fp_db, "trial") %>% 
  collect()

traits <- tbl(fp_db, "trait") %>% 
  collect() %>% 
  select(trait_id = id,
         trait= caption)

trait_ins <- tbl(fp_db, "traitInstance") %>% 
  collect()

trait_ins <- trait_ins %>% 
  select(traitInstance_id=`_id`,trait_id)

node <- tbl(fp_db, "node") %>% 
  collect()

node <- node %>% 
  select(node_id = id,
         row, col)

datum <- tbl(fp_db, "datum") %>% 
  collect()

datum <- datum %>% 
  mutate(ntime = as.integer(str_sub(timestamp,1,10)))

datum$ntime <- as.POSIXct(datum$ntime,origin="1970-01-01",tz="Australia/Perth")

datum <- datum %>% 
  left_join(node) %>% 
  arrange(row, col) %>% 
  left_join(trait_ins) %>% 
  left_join(traits) %>% 
  left_join(att) %>% 
  arrange(row, col, trait)

rm_col <- c("_id", "node_id","traitInstance_id","timestamp","gps_long","gps_lat","saved","trait_id")

datum <- datum %>% 
  select(-rm_col)

dup_dat <- datum %>% 
  group_by(node_id, trait) %>% 
  mutate(dup = 1:n()) %>% 
  mutate(trait_new = paste(trait, dup, sep="_")) %>% 
  ungroup() %>% 
  select(node_id, trait_new, value,row, col, plotname, unique_id, tos) %>% 
  spread(trait_new, value)

col_names_df <- data.frame(col_head = colnames(dup_dat)) %>%
  mutate(names = col_head) %>% 
  separate(names, paste0("n",1:6)) %>% 
  arrange(n1,n2,n3,n4,n5) %>% 
  filter(!col_head %in% c("row","col","node_id","plotname","unique_id","tos"))

write_csv(datum, "meredin_fp_database_file_02_long.csv", na="")
write_csv(dup_dat, "meredin_fp_database_file_02_wide.csv", na="")

