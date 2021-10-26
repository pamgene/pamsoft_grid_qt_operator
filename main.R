library(tercen)
library(plyr)
library(dplyr)

library(stringr)
library(jsonlite)


library(processx)


do.quant <- function(df, tmpDir){
  grpCluster <- unique(df$grdImageNameUsed)
  
  
  actual = get("actual",  envir = .GlobalEnv) + 1
  total = get("total",  envir = .GlobalEnv) 

  assign("actual", actual, envir = .GlobalEnv)
  
  if(!is.null(task)){
    evt = TaskProgressEvent$new()
    evt$taskId = task$id
    evt$message = paste0("Performing quantification (",  as.integer(100*(actual/2)/total),"%)")
    ctx$client$eventService$sendChannel(task$channelId, evt)
  }
  
  
  procList <- list()
  for(grp in grpCluster)
  {
    
    baseFilename <- paste0( tmpDir, "/", grp, "_")
    jsonFile <- paste0(baseFilename, '_param.json')

    #MCR_PATH <- "/home/rstudio/mcr/v99"
    MCR_PATH <- "/opt/mcr/v99"
    
    p<-processx::process$new("/mcr/exe/run_pamsoft_grid.sh", 
                             c(MCR_PATH, 
                               paste0("--param-file=", jsonFile[1])),
                             stdout = "|", stderr="|")
    
    procList <- append( procList, p )
  }
  
  
  # Wait for all processes to finish
  for(p in procList)
  {
    # Wait for 10 minutes then times out
    p$wait(timeout = 1000 * 60 * 10)
  }
  
  
  outDf <- NULL
  
  for(grp in grpCluster)
  {
    grd.ImageNameUsed = grp
    baseFilename <- paste0( tmpDir, "/", grd.ImageNameUsed, "_")
    jsonFile <- paste0(baseFilename, '_param.json')
    
    # The rest of the code should be very similar
    outputfile <- paste0(baseFilename, "_out.txt") 
    
    
    quantOutput <- read.csv(outputfile, header = TRUE)
    nGrid       <- nrow(quantOutput)
    

    inDf <- df %>% filter(grdImageNameUsed == grp)
    
    # Filter by a single variable here with filter
    inTable = inDf  %>% select(.ci, .ri, spotCol, spotRow, Image) %>% filter(.ri == 0 )
    
    quantOutput =  quantOutput %>% 
      rename(spotCol = Column) %>%
      rename(spotRow = Row) %>%
      rename(Image = ImageName) %>%
      mutate(across(where(is.numeric), as.double))
    
    
    quantOutput = quantOutput %>% left_join(inTable, by=c("spotCol", "spotRow", "Image")) %>%
      select(-spotCol, -spotRow, -Image, -.ri) 
    
    if(is.null(outDf)){
      outDf <- quantOutput
    }else{
      outDf <- rbind(outDf, quantOutput)
    }
    
    # Clean up
    jsonFile <- paste0(baseFilename, '_param.json')
    gridfile <- paste0(baseFilename, '_grid.txt')
    
    unlink(gridfile)
    unlink(jsonFile)
    unlink(outputfile)
  }
  
  if(!is.null(task)){
    evt = TaskProgressEvent$new()
    evt$taskId = task$id
    evt$message = paste0("Performing quantification (",  as.integer(100*(actual)/total),"%)")
    ctx$client$eventService$sendChannel(task$channelId, evt)
  }
  
  return(outDf)
}

run_quantification <- function(grdImageNameUsed, props, docId, imgInfo, tmpDir){
  grd.ImageNameUsed = grdImageNameUsed
  baseFilename <- paste0( tmpDir, "/", grd.ImageNameUsed, "_")
  jsonFile <- paste0(baseFilename, '_param.json')
  
  
  #MCR_PATH <- "/opt/mcr/v99"
  MCR_PATH <- "/home/rstudio/mcr/v99"
  
  
  #if( file.exists("/mcr/exe/run_pamsoft_grid.sh") ){
  if( file.exists("/home/rstudio/pg_exe/run_pamsoft_grid.sh") ){
    #system(paste0("/home/rstudio/pg_exe/run_pamsoft_grid.sh ", 
    #             MCR_PATH,
    #             " \"--param-file=", jsonFile[1], "\"", sep=""))
    
    p<-processx::process$new("/home/rstudio/pg_exe/run_pamsoft_grid.sh", 
                             c(MCR_PATH, 
                               paste0("--param-file=", jsonFile[1])),
                             stdout = "|", stderr="|")
  }else{
    # Set LD_LIBRARY_PATH environment variable to speed calling pamsoft_grid multiple times
    LIBPATH <- "."
    
    MCR_PATH_1 <- paste(MCR_PATH, "runtime", "glnxa64", sep = "/")
    MCR_PATH_2 <- paste(MCR_PATH, "bin", "glnxa64", sep = "/")
    MCR_PATH_3 <- paste(MCR_PATH, "sys", "os", "glnxa64", sep = "/")
    MCR_PATH_4 <- paste(MCR_PATH, "sys", "opengl", "lib", "glnxa64", sep = "/")
    
    LIBPATH <- paste(LIBPATH,MCR_PATH_1, sep = ":")
    LIBPATH <- paste(LIBPATH,MCR_PATH_2, sep = ":")
    LIBPATH <- paste(LIBPATH,MCR_PATH_3, sep = ":")
    LIBPATH <- paste(LIBPATH,MCR_PATH_4, sep = ":")
    
    Sys.setenv( "LD_LIBRARY_PATH" = LIBPATH ) 

    p<-processx::process$new("/mcr/exe/pamsoft_grid", 
                             c(paste0("--param-file=", jsonFile[1])))
  }
  
  
  return(p)
  
}

get_operator_props <- function(ctx, imagesFolder){
  sqcMinDiameter <- -1
  grdSpotPitch   <- -1
  grdSpotSize   <- -1
  
  operatorProps <- ctx$query$operatorSettings$operatorRef$propertyValues
  
  for( prop in operatorProps ){
    if (prop$name == "MinDiameter"){
      sqcMinDiameter <- prop$value
    }
    
    if (prop$name == "SpotPitch"){
      grdSpotPitch <- prop$value
    }
    
    if (prop$name == "SpotSize"){
      grdSpotSize <- prop$value
    }
  }
  
  if( is.null(grdSpotPitch) || grdSpotPitch == -1 ){
    grdSpotPitch <- 21.5
  }
  
  if( is.null(grdSpotSize) || grdSpotSize == -1 ){
    grdSpotSize <- 0.66
  }
  
  if( is.null(sqcMinDiameter) || sqcMinDiameter == -1 ){
    sqcMinDiameter <- 0.45
  }
  
  props <- list()
  
  props$sqcMinDiameter <- sqcMinDiameter
  props$grdSpotPitch <- grdSpotPitch
  props$grdSpotSize <- grdSpotSize
  
  
  # Get array layout
  layoutDirParts <- str_split_fixed(imagesFolder, "/", Inf)
  nParts  <- length(layoutDirParts) -1 # Layout is in parent folder
  
  layoutDir = ''
  
  for( i in 1:nParts){
    layoutDir <- paste(layoutDir, layoutDirParts[i], sep = "/")
  }
  layoutDir <- paste(layoutDir, "*Layout*", sep = "/")
  
  props$arraylayoutfile <- Sys.glob(layoutDir)
  
  return (props)
}

remove_variable_ns <- function(varName){
  fname <- str_split(varName, '[.]', Inf)
  fext <- fname[[1]][2]
  
  return(fext)
}


prep_image_folder <- function(docId){
  task = ctx$task
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$total = 1
  evt$actual = 0
  evt$message = "Downloading image files"
  ctx$client$eventService$sendChannel(task$channelId, evt)
  
  #1. extract files
  doc   <- ctx$client$fileService$get(docId )
  filename <- tempfile()
  writeBin(ctx$client$fileService$download(docId), filename)
  
  on.exit(unlink(filename, recursive = TRUE, force = TRUE))
  
  image_list <- vector(mode="list", length=length(grep(".zip", doc$name)) )
  
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


prep_quant_files <- function(df, props, docId, imgInfo, grp, tmpDir){
  sqcMinDiameter       <- as.numeric(props$sqcMinDiameter) #0.45
  segEdgeSensitivity   <- list(0, 0.01)
  qntSeriesMode        <- 0
  qntShowPamGridViewer <- 0
  grdSpotPitch         <- as.numeric(props$grdSpotPitch) #21.5
  grdSpotSize          <- as.numeric(props$grdSpotSize) #21.5
  grdUseImage          <- "Last"
  pgMode               <- "quantification"
  dbgShowPresenter     <- "no"
  #-----------------------------------------------
  # END of property setting
  baseFilename <- paste0( tmpDir, "/", grp, "_")
  grd.ImageNameUsed = grp #df$grdImageNameUsed[[1]]
  
  
  # JUst need the image used for gridding, so we get the table from that
  gridImageUsedTable = df %>% filter(Image == grd.ImageNameUsed)
  gridImageUsedTable$variable = sapply(gridImageUsedTable$variable, remove_variable_ns)
  
  grdRow <- gridImageUsedTable %>% filter(variable == "grdXOffset") %>% pull(spotRow)
  grdCol <- gridImageUsedTable %>% filter(variable == "grdYOffset") %>% pull(spotCol)
  
  grdXOffset <- gridImageUsedTable %>% filter(variable == "grdXOffset") %>% pull(.y)
  grdYOffset <- gridImageUsedTable %>% filter(variable == "grdYOffset") %>% pull(.y)
  
  grdXFixedPosition <- gridImageUsedTable %>% filter(variable == "grdXFixedPosition") %>% pull(.y)
  grdYFixedPosition <- gridImageUsedTable %>% filter(variable == "grdYFixedPosition") %>% pull(.y)
  
  gridX <- gridImageUsedTable %>% filter(variable == "gridX") %>% pull(.y)
  gridY <- gridImageUsedTable %>% filter(variable == "gridY") %>% pull(.y)
  
  grdRotation <- gridImageUsedTable %>% filter(variable == "grdRotation") %>% pull(.y)
  
  qntSpotID <-gridImageUsedTable %>% filter(variable == "grdRotation") %>% pull(ID)
  grdIsReference <- rep(0,length(qntSpotID))
  for(i in seq_along(qntSpotID)) {
    if (qntSpotID[i] == "#REF"){
      grdIsReference[i] = 1
    }
  }
  
  imageList <- unique( pull(df, "Image" ) )
  
  for(i in seq_along(imageList)) {
    imageList[i] <- paste(imgInfo[1], imageList[i], sep = "/" )
    imageList[i] <- paste(imageList[i], imgInfo[2], sep = "." )
  }
  
  imageUsedPath <- paste(imgInfo[1],  grd.ImageNameUsed, sep = "/" )
  imageUsedPath <- paste(imageUsedPath, imgInfo[2], sep = "." )
  
  
  dfGrid <- data.frame(
    "qntSpotID"=qntSpotID,
    "grdIsReference"=grdIsReference,
    "grdRow"=grdRow,
    "grdCol"=grdCol,
    "grdXOffset"=grdXOffset,
    "grdYOffset"=grdYOffset,
    "grdXFixedPosition"=grdXFixedPosition,
    "grdYFixedPosition"=grdYFixedPosition,
    "gridX"=gridX,
    "gridY"=gridY,
    "grdRotation"=grdRotation,
    "grdImageNameUsed"=imageUsedPath
  )
  
  
  gridfile <- paste0(baseFilename, '_grid.txt') #tempfile(fileext=".txt") 
  #on.exit(unlink(gridfile))
  
  
  write.table(dfGrid, gridfile, quote=FALSE,sep=",", row.names = FALSE)
  
  # The rest of the code should be very similar
  outputfile <- paste0(baseFilename, '_out.txt') #tempfile(fileext=".txt") 
  #on.exit(unlink(outputfile))
  
  dfJson = list("sqcMinDiameter"=sqcMinDiameter, 
                "segEdgeSensitivity"=segEdgeSensitivity,
                "qntSeriesMode"=qntSeriesMode,
                "qntShowPamGridViewer"=qntShowPamGridViewer,
                "grdSpotPitch"=grdSpotPitch,
                "grdSpotSize"=grdSpotSize,
                "grdUseImage"=grdUseImage,
                "pgMode"=pgMode,
                "dbgShowPresenter"=dbgShowPresenter,
                "arraylayoutfile"=props$arraylayoutfile,
                "griddingoutputfile"=gridfile,
                "outputfile"=outputfile, "imageslist"=unlist(imageList))
  
  jsonData <- toJSON(dfJson, pretty=TRUE, auto_unbox = TRUE)
  
  jsonFile <- paste0(baseFilename, '_param.json') #tempfile(fileext = ".json")
  #on.exit(unlink(jsonFile))
  
  write(jsonData, jsonFile)
}


do.readout <- function(df, tmpDir ){
  grd.ImageNameUsed = df$grdImageNameUsed[1]
  baseFilename <- paste0( tmpDir, "/", grd.ImageNameUsed, "_")
  jsonFile <- paste0(baseFilename, '_param.json')
  
  grd.ImageNameUsed = df$grdImageNameUsed[[1]]
  
  # The rest of the code should be very similar
  outputfile <- paste0(baseFilename, "_out.txt") #tempfile(fileext=".txt") 
  
  
  quantOutput <- read.csv(outputfile, header = TRUE)
  nGrid       <- nrow(quantOutput)
  
  
  # Filter by a single variable here with filter
  inTable = df  %>% select(.ci, .ri, spotCol, spotRow, Image) %>% filter(.ri == 0 )
  
  quantOutput =  quantOutput %>% 
    rename(spotCol = Column) %>%
    rename(spotRow = Row) %>%
    rename(Image = ImageName) %>%
    mutate(across(where(is.numeric), as.double))
  
  
  quantOutput = quantOutput %>% left_join(inTable, by=c("spotCol", "spotRow", "Image")) %>%
    select(-spotCol, -spotRow, -Image, -.ri) 
  
  # Clean up
  jsonFile <- paste0(baseFilename, '_param.json')
  gridfile <- paste0(baseFilename, '_grid.txt')
  
  unlink(gridfile)
  unlink(jsonFile)
  unlink(outputfile)
  
  return(quantOutput)
}

# =====================
# MAIN OPERATOR CODE
# =====================
#http://127.0.0.1:5402/admin/w/378f18ac66a21562f6dc43c28401df71/ds/da68ad6d-2fbd-4a72-903c-68ce84607991
options("tercen.workflowId" = "378f18ac66a21562f6dc43c28401df71")
options("tercen.stepId"     = "da68ad6d-2fbd-4a72-903c-68ce84607991")

actual <- 0
assign("actual", actual, envir = .GlobalEnv)

ctx = tercenCtx()

task = ctx$task

if(!is.null(task)){
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$message = "Loading table"
  ctx$client$eventService$sendChannel(task$channelId, evt)
}

required.cnames = c("documentId","grdImageNameUsed","Image","spotRow","spotCol","ID")
required.rnames = c("variable")

cnames.with.ns = ctx$cnames
rnames.with.ns = ctx$rnames

# here we keep the order of required.cnames
required.cnames.with.ns = lapply(required.cnames, function(required.cname){
  Find(function(cname.with.ns){
    endsWith(cname.with.ns, required.cname)
  }, cnames.with.ns, nomatch=required.cname)
})

required.rnames.with.ns = lapply(required.rnames, function(required.rname){
  Find(function(rname.with.ns){
    endsWith(rname.with.ns, required.rname)
  }, rnames.with.ns, nomatch=required.rname)
})

cTable <- ctx$cselect(required.cnames.with.ns)
rTable <- ctx$rselect(required.rnames.with.ns)

# override the names
names(cTable) = required.cnames
names(rTable) = required.rnames


docId     <- unique( cTable["documentId"]  )[[1]]
imgInfo   <- prep_image_folder(docId)
props     <- get_operator_props(ctx, imgInfo[1])

qtTable <- ctx$select(c(".ci", ".ri", ".y"))
cTable[[".ci"]] = seq(0, nrow(cTable)-1)

qtTable = dplyr::left_join(qtTable,cTable,by=".ci")

rTable[[".ri"]] = seq(0, nrow(rTable)-1)

qtTable = dplyr::left_join(qtTable,rTable,by=".ri")


if(!is.null(task)){
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

while(k <= length(groups)){
  for(i in 1:nCores){
    queu <- append( queu, order )
    k <- k + 1
    if( k > length(groups)){break}
  }
  order <- order + 1
}


assign("total", max(queu), envir = .GlobalEnv)


if(!is.null(task)){
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$message = "Loading grid data"
  ctx$client$eventService$sendChannel(task$channelId, evt)
}
qtTable$queu <- mapvalues(qtTable$grdImageNameUsed, 
                               from=groups, 
                               to=unlist(queu) )

# Preparation step
qtTable %>% 
  group_by(grdImageNameUsed)   %>%
  group_walk(~ prep_quant_files(.x, props, docId, imgInfo, .y, tmpDir) ) 


if(!is.null(task)){
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$message = "Performing quantification (0%)"
  ctx$client$eventService$sendChannel(task$channelId, evt)
}

# Execution step
outTable <-qtTable %>% 
  group_by(queu)   %>%
  do(do.quant(., tmpDir)  ) %>%
  ungroup() %>%
  select(-queu) %>%
  arrange(.ci) %>%
  ctx$addNamespace() %>%
  ctx$save() 




  
  