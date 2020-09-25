# Step 0: set directories, libraries
rm(list = ls())

library(tidyverse)
library(data.table)
library(vegan)

outdir <- "Outputs"
datadir <- "/Volumes/jgephart"


# Step 1: Sum total production per taxa by year

# Just do all years
#prod_year_start <- 2012
#prod_year_end <- 2016

# Load function rebuild_fish

rebuild_fish <- function(path_to_zipfile) {
  require(tools) # needed for file_path_sans_ext
  require(dplyr)
  require(purrr)
  require(readxl) # part of tidyverse but still need to load readxl explicitly, because it is not a core tidyverse package
  
  # The following ensures unzipped folder is created in the same directory as the zip file (can be different from the working directory)
  # set outdir
  if (file.exists(basename(path_to_zipfile))) { # if file is in current directory and only file name was given
    outdir <- getwd()
  } else if (file.exists(path_to_zipfile)) { # if file path was given
    outdir <- dirname(path_to_zipfile)
  } else {
    stop("Check path_to_zipfile")
  }
  
  foldername <- file_path_sans_ext(basename(path_to_zipfile))
  outfolder <- paste(outdir, foldername, sep = "/")
  unzip(path_to_zipfile, exdir = outfolder) # Problem: if unable to unzip folder, still creates outfolder how to supress this?
  # setwd(outfolder)
  # list files
  fish_files <- list.files(outfolder)
  
  # read .xlsx file (explains data structure of time series)
  # IMPORTANT: column ORDER (ABCDEF) in DS file should match columns ABCDEF in time series for looping to work below
  # each row gives info for how this time series column should be merged with a code list (CL) file
  ds_file <- fish_files[grep("DSD", fish_files)]
  path_to_ds <- paste(outfolder, ds_file, sep = "/")
  
  # skip removes title row
  ds <- read_excel(path_to_ds, skip=1)
  
  # manually correct ds file's codelist ID column:
  ds <- ds %>%
    mutate(Codelist_Code_id = case_when(
      Concept_id == "SOURCE" ~ "IDENTIFIER",
      Concept_id == "SYMBOL" ~ "SYMBOL",
      Concept_id != "SYMBOL|SOURCE" ~ Codelist_Code_id
    ))
  
  # Multiple CL files have the following column names in common: "Identifier" and "Code"
  # Which means after merge, below, you get "Identifier.x" and "Identifier.y", etc.
  # To disambiguate, Append Codelist with Concept_id
  code_ids_to_change<-ds$Codelist_Code_id[grep("IDENTIFIER|CODE", ds$Codelist_Code_id)]
  concept_ids_to_append<-ds$Concept_id[grep("IDENTIFIER|CODE", ds$Codelist_Code_id)]
  new_code_ids <- paste(concept_ids_to_append, code_ids_to_change, sep = "_")
  ds$Codelist_Code_id[grep("IDENTIFIER|CODE", ds$Codelist_Code_id)]<-new_code_ids
  
  # remove non CSVs (do this to ignore "CL_History.txt" file)
  fish_files <- fish_files[grep(".csv", fish_files)]
  
  # read in time series.csv
  time_files <- fish_files[grep("TS", fish_files)]
  path_to_ts <- paste(outfolder, time_files, sep = "/")
  time_series <- read.csv(path_to_ts)
  names(time_series) <- tolower(names(time_series))
  time_series_join <- time_series
  
  for (i in 1:nrow(ds)) {
    # TRUE/FALSE: is there a filename listed in Codelist_id?
    if (!is.na(ds$Codelist_id[i])) {
      # Use ds file to generate path_to_cl individually
      code_file_i <- paste(ds$Codelist_id[i], ".csv", sep = "")
      path_to_cl <- paste(outfolder, code_file_i, sep = "/")
      cl_i <- read.csv(path_to_cl, check.names = FALSE) # check.names = FALSE to prevent R from adding "X" in front of column "3Alpha_Code" - creates problems because this is the matching column for merging with time series
      
      # Many CL files have "Name" as a column, also Name_En, Name_Fr, Name_Es, etc
      # Also, "Identifier", "Major Group", and "Code" are common across some CL files
      # To disambiguate, append "Concept_ID" from DS file to all columns in CL that contain these terms
      concept_names <- paste(ds$Concept_id[i], names(cl_i)[grep("Name|Major_Group|Identifier|Code", names(cl_i))], sep = "_")
      names(cl_i)[grep("Name|Major_Group|Identifier|Code", names(cl_i))] <- concept_names
      
      
      names(cl_i) <- tolower(names(cl_i)) # convert all cl headers to lowercase
      merge_col <- tolower(ds$Codelist_Code_id[i]) # do the same to DS file's code ID so it matches with cl
      
      
      # If factor...
      #if (is.factor(cl_i[[merge_col]])) {
      # ...Test if factor levels need to be merged?
      #if (!nlevels(cl_i[[merge_col]]) == nlevels(time_series_join[[names(time_series_join)[i]]])) {
      # combined <- sort(union(time_series_join[[names(time_series_join)[i]]], levels(cl_i[[merge_col]])))
      #    levels(time_series_join[[names(time_series_join)[i]]]) <- levels(cl_i[[merge_col]])
      #  }
      #}
      # This avoids warnings about unequal factor levels below
      
      # Try converting to character first instead
      if (is.factor(cl_i[[merge_col]])){
        cl_i[[merge_col]]<-as.character(cl_i[[merge_col]])
        time_series_join[[names(time_series_join)[i]]]<-as.character(time_series_join[[names(time_series_join)[i]]])
      }
      
      
      # Can't just merge by column number:
      # In Time Series, column COUNTRY, AREA, SOURCE, SPECIES, and UNIT correspond to column 1 in their respective CL files
      # but in Time Series, column SYMBOL corresponds to column 2
      
      # Note: the following code does not work: #time_series_join<-left_join(time_series, cl_i, by = c(names(time_series)[i] = merge_col))
      # the argument "by" needs to take on the form of join_cols as shown below
      firstname <- names(time_series_join)[i]
      join_cols <- merge_col
      names(join_cols) <- firstname
      
      
      time_series_join <- left_join(time_series_join, cl_i, by = join_cols)
      
      # Convert back to factor
      if (is.character(time_series_join[[names(time_series_join)[i]]])){
        time_series_join[[names(time_series_join)[i]]]<-as.factor(time_series_join[[names(time_series_join)[i]]])
      }
    }
    # Expected warning: Coerces from factor to character because time_series$SPECIES (nlevels=2341) and CL_FI_SPECIES_GROUPS.csv column "3alpha_code" (nlevels = 12751) have different number of factor levels
    # Expected warning: Coerces from factor to chracter because time_series$UNIT and CL_FILE_UNIT.csv column "code" have different number of factor levels
    # Expected warning: Coerces from factor to character because time_series$SYMBOL and CL_FI_SYMBOL.csv column "symbol" have diff number of factors
  }
  
  return(time_series_join)
}

fishstat_dat <- rebuild_fish(file.path(datadir, "FishStatR/Data/Production-Global/ZippedFiles/GlobalProduction_2019.1.0.zip"))

fishstat_by_year <- fishstat_dat %>% 
  #filter(year >= prod_year_start & year <= prod_year_end) %>%
  filter(unit == "t") %>%
  group_by(year, species_scientific_name) %>%
  summarise(total_t = sum(quantity)) %>%
  ungroup() %>%
  rename(SciName = species_scientific_name) %>%
  mutate(SciName = tolower(SciName))

# Step 2: Join with hs_taxa_match 
# Do this for different hs versions (e.g., for HS version 2012, use production data for 2012 to 2016)

# Eventually loop through hs_version and prod_year:
last_year <- max(fishstat_by_year$year)
prod_year <- seq(1992, last_year, 1)
hs_breaks <- c(1992, 1996, 2002, 2007, 2012, 2017)

years_between <- c(diff(hs_breaks), (last_year - 2017 + 1)) # number of times to repeat "HS version" after each hs_break year
hs_year <- rep(hs_breaks, times = years_between) # hs_breaks[-6] because 
hs_version <- paste("HS", substr(hs_year, 3, 4), sep = "")

hs_prod_year_match <- data.frame(cbind(prod_year = prod_year, hs_version = hs_version))

div_indices_list <- list()

for (i in 1:nrow(hs_prod_year_match)){
  # Set prod_year and hs_version for now:
  #prod_year_i <- 2012
  #hs_version_i <- "HS12"
  
  # Get prod_year and hs_version year
  prod_year_i <- hs_prod_year_match[i, "prod_year"]
  hs_version_i <- hs_prod_year_match[i, "hs_version"]

  hs_taxa_match <- read.csv(file.path(datadir, "ARTIS", "HS Taxa Matches", "match to FAO", paste("2020-09-23_hs-taxa-match_ver", hs_version_i, ".csv", sep = "")))
  
  diversity_dat <- hs_taxa_match %>%
    left_join(fishstat_by_year %>% filter(year == prod_year_i), by = "SciName") %>%
    #filter(Match_category %in% c()) %>% # Filter specific match categories before pivoting
    select(Code, SciName, total_t) %>% 
    mutate(total_t = replace_na(total_t, 0)) %>%
    pivot_wider(names_from = SciName, values_from = total_t) %>%
    mutate(across(everything(), ~replace_na(.x, 0)))
  
  # Step 3: Then calculate Shannon Diversity (others like Simpsons?), using code as the "plots or ecosystems" and production as the "counts/biomass per species"
  
  # See examples in diversity() help file:
  H <- diversity(diversity_dat[,-1], index = "shannon")
  simp <- diversity(diversity_dat[,-1], index = "simpson")
  invsimp <- diversity(diversity_dat[,-1], index = "inv")
  #unbias.simp <- rarefy(diversity_dat[,-1], 2) - 1 # error: requires counts (i.e., INTEGERS); note: could go back to FAO fishstat data and subset "number" instead of "tons" 
  
  # Fisher alpha
  #alpha <- fisher.alpha(diversity_dat[,-1]) # error: requires counts (i.e., INTEGERS)
  
  # Species richness (S) and Pielou's evenness (J):
  S <- specnumber(diversity_dat[,-1])
  J <- H/log(S)
  
  div_indices_year_i <- cbind.data.frame(code = diversity_dat$Code, 
                                         year = prod_year_i,
                                         hs_version = hs_version_i,
                                         shannon = H,
                                         simpson = simp,
                                         invsimp = invsimp,
                                         richness = S,
                                         evenness = J)
  
  div_indices_list[[i]] <- div_indices_year_i
}

div_indices_dt <- data.table::rbindlist(div_indices_list)

# Plot diversity indices per code through time
index <- c("shannon", "simpson", "invsimp", "richness", "evenness")

for (i in 1:length(index)){
  p <- ggplot(data = div_indices_dt) +
    geom_line(aes(x = year, y = !!as.name(index[i]), group = code)) +
    labs(title = paste(index[i], " index of trade codes based on global production through time", sep = "")) +
    theme_classic()
  
  plot(p)
  # Note: warning about missing values:
  # Comes from "evenness" index for trade codes that have one species ( evenness = shannon / log(richness)), when richness = 1, dividing by 0 leads to NaN
}

# LEFT OFF HERE: next add code family to data table? 0301, 0302, 0303 etc
# Then either color code plots or split plots by code family (~facet_wrap)

