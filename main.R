library(tercen)
library(tercenApi)
library(plyr)
library(dplyr)
library(stringr)
library(jsonlite)
library(processx)
library(parallelly)

library(tiff)
library(tim)
library(tools)
library(stringi)


source('aux_functions.R')


prep_quant_files <- function(df, props, docId, imgInfo, grp, tmpDir) {
  #tmpDir <- '/home/rstudio/projects/pamsoft_grid_qt_operator/tmp/'
  baseFilename <- paste0(tmpDir, "/", grp, "_")
  grd.ImageNameUsed = grp 

  # JUst need the image used for gridding, so we get the table from that
  image = df$Image[[1]]
  gridImageUsedTable = df %>% filter(Image == image)

  #gridImageUsedTable$variable = sapply(gridImageUsedTable$variable, remove_variable_ns)
  gridImageUsedTable$variable <- stri_split_fixed(gridImageUsedTable$variable, ".", 2, simplify = TRUE)[,2]

  grdRow <- gridImageUsedTable %>%
    filter(variable == "gridX") %>%
    pull(spotRow)
  grdCol <- gridImageUsedTable %>%
    filter(variable == "gridY") %>%
    pull(spotCol)


  grdXFixedPosition <- gridImageUsedTable %>%
    filter(variable == "grdXFixedPosition") %>%
    pull(.y)
  grdYFixedPosition <- gridImageUsedTable %>%
    filter(variable == "grdYFixedPosition") %>%
    pull(.y)
  
  diameter <- gridImageUsedTable %>%
    filter(variable == "diameter") %>%
    pull(.y)

  gridX <- gridImageUsedTable %>%
    filter(variable == "gridX") %>%
    pull(.y)
  gridY <- gridImageUsedTable %>%
    filter(variable == "gridY") %>%
    pull(.y)

  grdRotation <- gridImageUsedTable %>%
    filter(variable == "grdRotation") %>%
    pull(.y)
  
  isManual <- gridImageUsedTable %>%
    filter(variable == "manual") %>%
    pull(.y)  %>% as.logical() %>% as.integer()
  

  isBad <- gridImageUsedTable %>%
    filter(variable == "bad") %>%
    pull(.y)  %>% as.logical() %>% as.integer()
  

  isEmpty <- gridImageUsedTable %>%
    filter(variable == "empty") %>%
    pull(.y)  %>% as.logical() %>% as.integer()
  

  
  qntSpotID <- gridImageUsedTable %>%
    filter(variable == "grdRotation") %>%
    pull(ID)
  
  grdIsReference <- rep(0, length(qntSpotID))
  for (i in seq_along(qntSpotID)) {
    if (qntSpotID[i] == "#REF") {
      grdIsReference[i] = 1
    }
  }

  imageList <- unique(pull(df, "Image"))

  for (i in seq_along(imageList)) {
    imageList[i] <- paste(imgInfo[1], imageList[i], sep = "/")
    imageList[i] <- paste(imageList[i], imgInfo[2], sep = ".")
  }

  imageUsedPath <- paste(imgInfo[1], grd.ImageNameUsed, sep = "/")
  imageUsedPath <- paste(imageUsedPath, imgInfo[2], sep = ".")


  dfGrid <- data.frame(
    "qntSpotID" = qntSpotID,
    "grdIsReference" = grdIsReference,
    "grdRow" = grdRow,
    "grdCol" = grdCol,
    "grdXFixedPosition" = grdXFixedPosition,
    "grdYFixedPosition" = grdYFixedPosition,
    "gridX" = gridX,
    "gridY" = gridY,
    "diameter" = diameter,
    "grdRotation" = grdRotation,
    "grdImageNameUsed" = imageUsedPath,
    "isManual" = isManual,
    "isBad" = isBad,
    "isEmpty" = isEmpty
  )


  gridfile <- paste0(baseFilename, '_grid.txt') 

  write.table(dfGrid, gridfile, quote = FALSE, sep = ",", row.names = FALSE)

   outputfile <- paste0(baseFilename, '_out.txt') 
 

  if (length(imageList) > 1) {
    imageList <- unlist(imageList)
  }else {
    imageList <- list(imageList)
  }


  dfJson = list("sqcMinDiameter"=props$sqcMinDiameter,
                "sqcMaxDiameter"=props$sqcMaxDiameter,
                "segEdgeSensitivity"=props$segEdgeSensitivity,
                "qntSeriesMode"=0,
                "qntShowPamGridViewer"=0,
                "grdSpotPitch"=props$grdSpotPitch,
                "grdSpotSize"=props$grdSpotSize,
                "grdRotation"=props$grdRotation,
                "qntSaturationLimit"=props$qntSaturationLimit,
                "segMethod"=props$segMethod,
                "grdUseImage"="Last",
                "pgMode"="quantification",
                "dbgShowPresenter"=0,
                "arraylayoutfile"=props$arraylayoutfile,
                "griddingoutputfile"=gridfile,
                "outputfile"=outputfile, "imageslist"=unlist(imageList))
  

  jsonData <- toJSON(dfJson, pretty = TRUE, auto_unbox = TRUE)

  jsonFile <- paste0(baseFilename, '_param.json') 

  write(jsonData, jsonFile)
}

do.quant <- function(df, tmpDir) {
  ctx = tercenCtx()
  task = ctx$task

  grpCluster <- unique(df$grdImageNameUsed)

  actual = get("actual", envir = .GlobalEnv) + 1
  total = get("total", envir = .GlobalEnv)
  assign("actual", actual, envir = .GlobalEnv)

  procList = lapply(grpCluster, function(grp) {
    baseFilename <- paste0(tmpDir, "/", grp, "_")
    jsonFile <- paste0(baseFilename, '_param.json')
    MCR_PATH <- "/opt/mcr/v99"


    outLog <- tempfile(fileext=".log")
    p <- processx::process$new("/mcr/exe/run_pamsoft_grid.sh",
                               c(MCR_PATH,
                                 paste0("--param-file=", jsonFile[1])),
                               stdout = outLog)
    

    return(list(p = p, out = outLog))
  })

  # Wait for all processes to finish
  for (pObj in procList)
  {
    
    # Wait for 10 minutes then times out
    pObj$p$wait(timeout = 1000 * 60 * 10)
    exitCode <- pObj$p$get_exit_status()

    if (exitCode != 0) {
      for (pObj2 in procList) {
        if (pObj2$p$is_alive()) {
          print(paste0('kill process -- ' ))
          print(pObj$p)
          pObj2$p$kill()
          
        }
      }
      
      stop(readChar(pObj$out, file.info(pObj$out)$size))
    }
  }

  outDf <- NULL

  for (grp in grpCluster)
  {
    grd.ImageNameUsed = grp
    baseFilename <- paste0(tmpDir, "/", grd.ImageNameUsed, "_")

    # The rest of the code should be very similar
    outputfile <- paste0(baseFilename, "_out.txt")

    quantOutput <- read.csv(outputfile, header = TRUE)

    inDf <- df %>% filter(grdImageNameUsed == grp)

    # Filter by a single variable here with filter
    inTable = inDf %>%
      select(.ci, .ri, spotCol, spotRow, Image) %>%
      filter(.ri == 0)


    quantOutput = quantOutput %>%
      rename(spotCol = Column) %>%
      rename(spotRow = ROW) %>%
      rename(Image = ImageName) %>%
      mutate(across(where(is.numeric), as.double))

    quantOutput = quantOutput %>%
      left_join(inTable, by = c("spotCol", "spotRow", "Image")) %>%
      select(-spotCol, -spotRow, -Image, -.ri)

    if( props$isDiagnostic == FALSE){
      quantOutput <- quantOutput %>% 
        select(-Bad_Spot, -Diameter, -Empty_Spot, -Fraction_Ignored,
               -Position_Offset, -Replaced_Spot, -Mean_Background, -Mean_SigmBg,
               -Mean_Signal )
    }

    if (is.null(outDf)) {
      outDf <- quantOutput
    }else {
      outDf <- rbind(outDf, quantOutput)
    }

    # Clean up
    jsonFile <- paste0(baseFilename, '_param.json')
    gridfile <- paste0(baseFilename, '_grid.txt')

    unlink(gridfile)
    unlink(jsonFile)
    unlink(outputfile)
  }

  if (!is.null(task)) {
    evt = TaskProgressEvent$new()
    evt$taskId = task$id
    evt$total = total
    evt$actual = actual
    evt$message = paste0("Performing quantification: ", actual, "/", total)
    ctx$client$eventService$sendChannel(task$channelId, evt)
  }

  return(outDf)
}



# =====================
# MAIN OPERATOR CODE
# =====================
#http://127.0.0.1:5402/admin/w/2e726ebfbecf78338faf09317803614c/ds/bdb3b164-3a0e-4aae-a03d-f275ebb2395a
# After review
# http://127.0.0.1:5402/test-team/w/cc41c236da58dcb568c6fe1a320140d2/ds/961ce284-0eec-48d8-825a-a95728e5678c
# options("tercen.workflowId" = "cc41c236da58dcb568c6fe1a320140d2")
# options("tercen.stepId" = "961ce284-0eec-48d8-825a-a95728e5678c")

actual <- 0
assign("actual", actual, envir = .GlobalEnv)

ctx = tercenCtx()

task = ctx$task

if (!is.null(task)) {
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$message = "Loading table"
  ctx$client$eventService$sendChannel(task$channelId, evt)
}

required.cnames = c("documentId", "grdImageNameUsed", "Image", "spotRow", "spotCol", "ID")
required.rnames = c("variable")

cnames.with.ns = ctx$cnames
rnames.with.ns = ctx$rnames



# Check if all columns exist exist
if(!all(unlist(  lapply(required.cnames, function(x)  any(grepl(x,  cnames.with.ns)) )))){
  msg <- paste0( "The following are required columns: ", unlist(paste0(required.cnames, sep=', ', collapse = '')))
  msg <- substr(msg, 1, nchar(msg)-2)
                 
  stop(msg)
}
  

# Check if row is setup correctly
if(!all(unlist(  lapply(required.rnames, function(x)  any(grepl(x,  rnames.with.ns)) )))){
  msg <- paste0( "The following are required rows: ", unlist(paste0(required.rnames, sep=', ', collapse = '')))
  msg <- substr(msg, 1, nchar(msg)-2)
  
  stop(msg)
}



# here we keep the order of required.cnames
required.cnames.with.ns = lapply(required.cnames, function(required.cname) {
  Find(function(cname.with.ns) {
    endsWith(cname.with.ns, required.cname)
  }, cnames.with.ns, nomatch = required.cname)
})

required.rnames.with.ns = lapply(required.rnames, function(required.rname) {
  Find(function(rname.with.ns) {
    endsWith(rname.with.ns, required.rname)
  }, rnames.with.ns, nomatch = required.rname)
})



cTable <- ctx$cselect(required.cnames.with.ns)
rTable <- ctx$rselect(required.rnames.with.ns)

# override the names
names(cTable) = required.cnames
names(rTable) = required.rnames

# Ensure all variables have been selected in the gather step
variables <- rTable %>% pull(variable)
req.variables <- c("grdRotation", "grdXFixedPosition", "grdYFixedPosition", "gridX", "gridY", "diameter", 
  "bad", "empty",  "manual")

if( !all(unlist(lapply( req.variables, function(x){ any(grepl(x, variables))  }  ))) ){
  msg <- paste0( "The following are variables are required from a Gather step: ", unlist(paste0(req.variables, sep=', ', collapse = '')))
  msg <- substr(msg, 1, nchar(msg)-2)
  
  stop(msg)
}



docId <- unique(cTable["documentId"])[[1]]
imgInfo <- prep_image_folder(docId)
props <- get_operator_props(ctx, imgInfo[1])

qtTable <- ctx$select(c(".ci", ".ri", ".y"))


cTable[[".ci"]] = seq(0, nrow(cTable) - 1)

qtTable = dplyr::left_join(qtTable, cTable, by = ".ci")

rTable[[".ri"]] = seq(0, nrow(rTable) - 1)

qtTable = dplyr::left_join(qtTable, rTable, by = ".ri")


if (!is.null(task)) {
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
}

tmpDir <- tempdir()


# Prepare processor queu
groups <- unique(qtTable$grdImageNameUsed)
nCores <- parallelly::availableCores(methods="cgroups.cpuset")
queu <- list()

currentCore <- 1
order <- 1
k <- 1

while (k <= length(groups)) {
  for (i in 1:nCores) {
    queu <- append(queu, order)
    k <- k + 1
    if (k > length(groups)) { break }
  }
  order <- order + 1
}


assign("total", max(unlist(queu)), envir = .GlobalEnv)

qtTable$queu <- mapvalues(qtTable$grdImageNameUsed,
                          from = groups,
                          to = unlist(queu))


# Preparation step
qtTable %>%
  group_by(grdImageNameUsed) %>%
  group_walk(~prep_quant_files(.x, props, docId, imgInfo, .y, tmpDir))


if (!is.null(task)) {
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$total = max(unlist(queu))
  evt$actual = 0
  evt$message = paste0("Performing quantification: ", actual, "/", total)
  ctx$client$eventService$sendChannel(task$channelId, evt)
}

# Execution step
qtTable %>%
  group_by(queu) %>%
  do(do.quant(., tmpDir)) %>%
  ungroup() %>%
  select(-queu) %>%
  arrange(.ci) %>%
  ctx$addNamespace() %>%
  ctx$save()
