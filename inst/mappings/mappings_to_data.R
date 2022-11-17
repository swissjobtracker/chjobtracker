# Helper to read in the mapping xlsx file and create packageable data
library(data.table)

isco <- as.data.table(readxl::read_excel("inst/mappings/x28_Mappings_2022-08-16.xlsx", sheet = 2))
setnames(isco, c("idx28", "namex28", "isco_5d", "nameisco"))
isco[, idx28 := as.numeric(idx28)]
isco[, isco_5d := as.numeric(isco_5d)]
isco <- isco[isco_5d >= 10000] # exclude military occupations
isco[, c("isco_1d", "isco_2d") := .(floor(isco_5d / 10000), floor(isco_5d / 1000))]
x28_isco <- isco[, .(idx28, namex28, isco_1d, isco_2d, isco_5d, nameisco)]
usethis::use_data(x28_isco, overwrite = TRUE)


noga <- as.data.table(readxl::read_excel("inst/mappings/x28_Mappings_2022-08-16.xlsx", sheet = 4))
setnames(noga, c("noga_full", "namenoga", "idx28", "namex28"))
noga[, idx28 := as.numeric(idx28)]
noga[, noga_letter := substr(noga_full, 1, 1)]

x28_noga <- unique(noga[, .(idx28, namex28, noga_letter)])

# Noga a and B have the same idx28, also description doesnt fit B (mining), only use A
x28_noga <- x28_noga[noga_letter != "B"][noga_letter == "A", noga_letter := "AB"]

# Noga S and T  have the same idx28, only use S
x28_noga <- x28_noga[noga_letter != "T"][noga_letter == "S", noga_letter := "ST"]



usethis::use_data(x28_noga, overwrite = TRUE)
