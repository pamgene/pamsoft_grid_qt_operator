library(tercen)
library(plyr)
library(dplyr)
library(stringr)
library(jsonlite)
library(processx)

prep_quant_files <- function(df, props, docId, imgInfo, grp, tmpDir) {

  sqcMinDiameter <- as.numeric(props$sqcMinDiameter) #0.45
  segEdgeSensitivity <- list(0, 0.01)
  qntSeriesMode <- 0
  qntShowPamGridViewer <- 0
  grdSpotPitch <- props$grdSpotPitch #21.5
  grdSpotSize <- props$grdSpotSize #21.5
  qntSaturationLimit <- props$qntSaturationLimit
  grdUseImage <- "Last"
  pgMode <- "quantification"
  dbgShowPresenter <- "no"
  #-----------------------------------------------
  # END of property setting
  baseFilename <- paste0(tmpDir, "/", grp, "_")
  grd.ImageNameUsed = grp 

  # JUst need the image used for gridding, so we get the table from that
  image = df$Image[[1]]
  gridImageUsedTable = df %>% filter(Image == image)

  gridImageUsedTable$variable = sapply(gridImageUsedTable$variable, remove_variable_ns)

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

  dfJson = list("sqcMinDiameter" = sqcMinDiameter,
                "segEdgeSensitivity" = segEdgeSensitivity,
                "qntSeriesMode" = qntSeriesMode,
                "qntShowPamGridViewer" = qntShowPamGridViewer,
                "qntSaturationLimit" = qntSaturationLimit,
                "grdSpotPitch" = grdSpotPitch,
                "grdSpotSize" = grdSpotSize,
                "grdUseImage" = grdUseImage,
                "pgMode" = pgMode,
                "dbgShowPresenter" = dbgShowPresenter,
                "arraylayoutfile" = props$arraylayoutfile,
                "griddingoutputfile" = gridfile,
                "outputfile" = outputfile, "imageslist" = imageList)

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


get_operator_props <- function(ctx, imagesFolder) {
  sqcMinDiameter <- -1
  grdSpotPitch <- -1
  grdSpotSize <- -1
  qntSaturationLimit <- -1
  isDiagnostic <- -1

  operatorProps <- ctx$
    query$
    operatorSettings$
    operatorRef$
    propertyValues

  for (prop in operatorProps) {
    if (prop$name == "Min Diameter") {
      sqcMinDiameter <- as.numeric(prop$value)
    }

    if (prop$name == "Spot Pitch") {
      grdSpotPitch <- as.numeric(prop$value)
    }

    if (prop$name == "Spot Size") {
      grdSpotSize <- as.numeric(prop$value)
    }
    
    if (prop$name == "Saturation Limit") {
      qntSaturationLimit <- as.numeric( prop$value )
    }
    
    if (prop$name == "Diagnostic Output") {
      isDiagnostic <- prop$value
    }
    
  }

  if (is.null(grdSpotPitch) || grdSpotPitch == -1) {
    grdSpotPitch <- 21.5
  }

  if (is.null(grdSpotSize) || grdSpotSize == -1) {
    grdSpotSize <- 0.66
  }

  if (is.null(sqcMinDiameter) || sqcMinDiameter == -1) {
    sqcMinDiameter <- 0.45
  }
  
  if (is.null(qntSaturationLimit) || qntSaturationLimit == -1) {
    qntSaturationLimit <- 2^12 - 1
  }
  
  if (is.null(isDiagnostic) || isDiagnostic == -1) {
    isDiagnostic <- "Yes"
  }
  
  if( isDiagnostic =="No"){
    isDiagnostic = FALSE
  }else{
    isDiagnostic = TRUE
  }

  props <- list()

  props$sqcMinDiameter <- sqcMinDiameter
  props$grdSpotPitch <- grdSpotPitch
  props$grdSpotSize <- grdSpotSize
  props$qntSaturationLimit <- qntSaturationLimit
  props$isDiagnostic <- isDiagnostic


  # Get array layout
  layoutDirParts <- str_split_fixed(imagesFolder, "/", Inf)
  nParts <- length(layoutDirParts) - 1 # Layout is in parent folder

  layoutDir = ''

  for (i in 1:nParts) {
    layoutDir <- paste(layoutDir, layoutDirParts[i], sep = "/")
  }
  layoutDir <- paste(layoutDir, "*Layout*", sep = "/")

  props$arraylayoutfile <- Sys.glob(layoutDir)

  return(props)
}

remove_variable_ns <- function(varName) {
  fname <- str_split(varName, '[.]', Inf)
  fext <- fname[[1]][2]

  return(fext)
}


prep_image_folder <- function(docId) {
  task = ctx$task
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$total = 1
  evt$actual = 0
  evt$message = "Downloading image files"
  ctx$client$eventService$sendChannel(task$channelId, evt)

  #1. extract files
  doc <- ctx$client$fileService$get(docId)
  filename <- tempfile()
  writeBin(ctx$client$fileService$download(docId), filename)

  on.exit(unlink(filename, recursive = TRUE, force = TRUE))

  image_list <- vector(mode = "list", length = length(grep(".zip", doc$name)))

  # unzip archive (which presumably exists at this point)
  tmpdir <- tempfile()
  unzip(filename, exdir = tmpdir)

  imageResultsPath <- file.path(list.files(tmpdir, full.names = TRUE), "ImageResults")

  f.names <- list.files(imageResultsPath, full.names = TRUE)

  fdir <- str_split_fixed(f.names[1], "/", Inf)
  fdir <- fdir[length(fdir)]

  fname <- str_split(fdir, '[.]', Inf)
  fext <- fname[[1]][2]

  # Images for all series will be here
  return(list(imageResultsPath, fext))

}


# =====================
# MAIN OPERATOR CODE
# =====================
#http://localhost:5402/admin/w/c41add34d78230432ba802d98801cec3/ds/78c6b6d3-b857-4fed-8a97-a25e45ca06a6
# options("tercen.workflowId" = "c41add34d78230432ba802d98801cec3")
# options("tercen.stepId" = "78c6b6d3-b857-4fed-8a97-a25e45ca06a6")

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
nCores <- parallel::detectCores()
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
df <- qtTable %>%
  group_by(queu) %>%
  do(do.quant(., tmpDir)) %>%
  ungroup() %>%
  select(-queu) %>%
  arrange(.ci) %>%
  ctx$addNamespace() %>%
  ctx$save()


