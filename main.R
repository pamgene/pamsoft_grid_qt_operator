library(tercen)
library(dplyr)

library(stringr)
library(jsonlite)


get_operator_props <- function(ctx, imagesFolder){
  sqcMinDiameter <- -1
  grdSpotPitch   <- -1
  
  operatorProps <- ctx$query$operatorSettings$operatorRef$propertyValues
  
  for( prop in operatorProps ){
    if (prop$name == "sqcMinDiameter"){
      sqcMinDiameter <- prop$value
    }
    
    if (prop$name == "grdSpotPitch"){
      grdSpotPitch <- prop$value
    }
  }
  
  props <- list()
  
  props$sqcMinDiameter <- sqcMinDiameter
  props$grdSpotPitch <- grdSpotPitch
  
  
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


prep_image_folder <- function(docId){
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

vec_last <- function(x) { return(x[length(x)]) }

get_column_with_namespace <- function(colName, colNameList){
  namespace <- ""
  fullColName <- ""
  
  
  for (i in 1:length(colNameList)){
    firstChar <- substring(colNameList[i], 1, 1)
    nameParts <- str_split_fixed(colNameList[i], "\\.", Inf)
    colName_ <- vec_last(nameParts)
    if ( firstChar != "." &&  grepl( colName, colName_) == TRUE){
      # Get the previous namespace
      namespace <-nameParts[1]
      fullColName <- colNameList[i]
    }
  }
  
  return( c(fullColName, namespace))
}



do.quant <- function(df, props, docId, imgInfo){
  sqcMinDiameter       <- 0.45 #as.numeric(props$sqcMinDiameter) #0.45
  segEdgeSensitivity   <- list(0, 0.01)
  qntSeriesMode        <- 0
  qntShowPamGridViewer <- 0
  grdSpotPitch         <- 21.5 #as.numeric(props$grdSpotPitch) #21.5
  grdUseImage          <- "Last"
  pgMode               <- "quantification"
  dbgShowPresenter     <- "no"
  #-----------------------------------------------
  # END of property setting
  
  
  scolNames <- ctx$cselect()
  rowNames  <- ctx$rselect()
  colNames  <- names(df)
  
  
  
  imageCol <- get_column_with_namespace("\\<grdImageNameUsed\\>", names(scolNames))
  
  
  imageName <- scolNames[imageCol[1]]
  imageName <- imageName[[1]][1]
  
  colInfo <- ctx$cselect() %>% filter((!!sym(imageCol[1])) ==  df[[imageCol[1]]][1])
  uImage <- unique( pull(colInfo, "ds0.Image" ) )
  
  qntIdCol <- get_column_with_namespace("qntSpotID", names(scolNames))

  qntSpotID <- colInfo[[qntIdCol[1]]]
  grdIsReference <- rep(0,length(qntSpotID))
  for(i in seq_along(qntSpotID)) {
    if (qntSpotID[i] == "#REF"){
      grdIsReference[i] = 1
    }
  }
  
  ciNamespace <- get_column_with_namespace("grdRow", names(colInfo))[2]
  gatherCol <- get_column_with_namespace("variable", colNames)[1]
  
  grdRow <- colInfo[[paste(ciNamespace, "grdRow", sep=".")]]
  grdCol <- colInfo[[paste(ciNamespace, "grdCol", sep=".")]]
  
  #Normally gs0.variable containing the variable names from the previous gather step
  # i.e. the resulting columns of the gridding operation
  grdXOffset <- df %>% filter((!!sym(gatherCol[1])) == paste(ciNamespace, "grdXOffset", sep=".") ) %>% pull(.y)
  grdYOffset <- df %>% filter( (!!sym(gatherCol[1])) == paste(ciNamespace, "grdYOffset", sep=".") ) %>% pull(.y)
  
  
  grdXFixedPosition <- df %>% filter( (!!sym(gatherCol[1])) == paste(ciNamespace, "grdXFixedPosition", sep=".") ) %>% pull(.y)
  grdYFixedPosition <- df %>% filter( (!!sym(gatherCol[1])) == paste(ciNamespace, "grdYFixedPosition", sep=".") ) %>% pull(.y)
  
  gridX <- df %>% filter( (!!sym(gatherCol[1])) == paste(ciNamespace, "gridX", sep=".") ) %>% pull(.y)
  gridY <- df %>% filter( (!!sym(gatherCol[1])) == paste(ciNamespace, "gridY", sep=".") ) %>% pull(.y)
  
  grdRotation <- df %>% filter( (!!sym(gatherCol[1])) == paste(ciNamespace, "grdRotation", sep=".") ) %>% pull(.y)
  grdImageNameUsed <- colInfo[[paste(ciNamespace, "grdImageNameUsed", sep=".")]]
  
  
  imageAppCol <- get_column_with_namespace("\\<Image\\>", names(scolNames))[1]
  
  imageList <- uImage #pull( colInfo, imageAppCol) 
  
  
  for(i in seq_along(imageList)) {
    imageList[i] <- paste(imgInfo[1], imageList[i], sep = "/" )
    imageList[i] <- paste(imageList[i], imgInfo[2], sep = "." )
  }
  
  
  imageUsedPath <- paste(imgInfo[1], grdImageNameUsed, sep = "/" )
  imageUsedPath <- paste(imageUsedPath, imgInfo[2], sep = "." )
  
  uIdx <- 1:(length(qntSpotID)/length(imageList))
  
  
  
  dfGrid <- data.frame(
    "qntSpotID"=qntSpotID[uIdx],
    "grdIsReference"=grdIsReference[uIdx],
    "grdRow"=grdRow[uIdx],
    "grdCol"=grdCol[uIdx],
    "grdXOffset"=grdXOffset[uIdx],
    "grdYOffset"=grdYOffset[uIdx],
    "grdXFixedPosition"=grdXFixedPosition[uIdx],
    "grdYFixedPosition"=grdYFixedPosition[uIdx],
    "gridX"=gridX[uIdx],
    "gridY"=gridY[uIdx],
    "grdRotation"=grdRotation[uIdx],
    "grdImageNameUsed"=imageUsedPath[uIdx]
  )
  
  
  gridfile <- tempfile(fileext=".txt") 
  on.exit(unlink(gridfile))
  
  
  write.table(dfGrid, gridfile, quote=FALSE,sep=",", row.names = FALSE)
  
  outputfile <- tempfile(fileext=".txt") 
  on.exit(unlink(outputfile))
  
  dfJson = list("sqcMinDiameter"=sqcMinDiameter, 
                "segEdgeSensitivity"=segEdgeSensitivity,
                "qntSeriesMode"=qntSeriesMode,
                "qntShowPamGridViewer"=qntShowPamGridViewer,
                "grdSpotPitch"=grdSpotPitch,
                "grdUseImage"=grdUseImage,
                "pgMode"=pgMode,
                "dbgShowPresenter"=dbgShowPresenter,
                "arraylayoutfile"=props$arraylayoutfile,
                "griddingoutputfile"=gridfile,
                "outputfile"=outputfile, "imageslist"=unlist(imageList))
  
  jsonData <- toJSON(dfJson, pretty=TRUE, auto_unbox = TRUE)
  
  jsonFile <- tempfile(fileext = ".json")
  on.exit(unlink(jsonFile))
  
  write(jsonData, jsonFile)
  
  system(paste("/mcr/exe/pamsoft_grid \"--param-file=", jsonFile[1], "\"", sep=""))
  
  
  quantOutput <- read.csv(outputfile, header = TRUE)
  nGrid       <- nrow(quantOutput)
  

  
  dfCol <- df %>% filter( gs0.variable == "ds1.grdRotation" ) %>% pull(.ci)
  dfRow <- df %>% filter( gs0.variable == "ds1.grdRotation" ) %>% pull(.ri)
  img <- colInfo %>%  pull(ds0.Image)
  
  nImg <- length(gridX)/length(uIdx)

  oMeanSigmBg <- quantOutput$Mean_SigmBg
  oMedianSigmBg <- oMeanSigmBg
  oRseMedianSigmBg <- oMeanSigmBg
  oMeanSignal <- oMeanSigmBg
  oMedianSignal <- oMeanSigmBg
  oStdSignal <- oMeanSigmBg
  oSumSignal <- oMeanSigmBg
  oRse_Signal <- oMeanSigmBg
  oMeanBackground <- oMeanSigmBg
  oMedianBackground <- oMeanSigmBg
  oStdBackground <- oMeanSigmBg
  oSumBackground <- oMeanSigmBg
  oRseBackground <- oMeanSigmBg
  oSignalSaturation <- oMeanSigmBg
  oFracIgnored <- oMeanSigmBg
  oDiameter <- oMeanSigmBg
  oPosOffset <- oMeanSigmBg
  oEmptySpot <- oMeanSigmBg
  oBadSpot <- oMeanSigmBg
  oReplacedSpot <- oMeanSigmBg
  
  oRow <- oMeanSigmBg
  oCol <- oMeanSigmBg
  oImg <- img
  oDel <- oMeanSigmBg
  oIsRef <- oMeanSigmBg
  
  colI <- oMeanSigmBg
  rowI <- oMeanSigmBg
  
  k <- 1
  for (z in 1:7){
    sids <- oMeanSigmBg
    for( j in 1:nImg ){
      
      for( w in 1:(length(uIdx))){
        i <- w + ((j-1)*length(uIdx)) 

        
        sids[k] <- w
        
        oMeanSigmBg[k] <- quantOutput$Mean_SigmBg[i]
        
        oMedianSigmBg[k] <- quantOutput$Median_SigmBg[i]
        oRseMedianSigmBg[k] <- quantOutput$Rse_MedianSigmBg[i]
        oMeanSignal[k] <- quantOutput$Mean_Signal[i]
        oMedianSignal[k] <- quantOutput$Median_Signal[i]
        oStdSignal[k] <- quantOutput$Std_Signal[i]
        oSumSignal[k] <- quantOutput$Sum_Signal[i]
        oRse_Signal[k] <- quantOutput$Rse_Signal[i]
        oMeanBackground[k] <- quantOutput$Mean_Background[i]
        oMedianBackground[k] <- quantOutput$Median_Background[i]
        oStdBackground[k] <- quantOutput$Std_Background[i]
        oSumBackground[k] <- quantOutput$Sum_Background[i]
        oRseBackground[k] <- quantOutput$Rse_Background[i]
        oSignalSaturation[k] <- quantOutput$Signal_Saturation[i]
        oFracIgnored[k] <- quantOutput$Fraction_Ignored[i]
        oDiameter[k] <- quantOutput$Diameter[i]
        oPosOffset[k] <- quantOutput$Position_Offset[i]
        oEmptySpot[k] <- quantOutput$Empty_Spot[i]
        oBadSpot[k] <- quantOutput$Bad_Spot[i]
        oReplacedSpot[k] <- quantOutput$Replaced_Spot[i]
        
        oRow[k] <- quantOutput$Row[i]
        oCol[k] <- quantOutput$Col[i]
        oImg[k] <- img[i]
        
        colI[k] <- df$.ci[i]
        
        if(quantOutput$Row[i] < 0){
          oIsRef[k] <- 1
        }
        else{
          oIsRef[k] <- 0
        }
        
        if(j == 1 && z == 1){
          oDel[k] <- 1
        }else{
          oDel[k] <- 0
        }
        
        k <- k + 1
      }
    }
  }

  outFrame <- data.frame( 
    .ci = colI,
    ds0.Image = oImg,
    rFac = oDel,
    MeanSigmBg = oMeanSigmBg,
    MedianSigmBg = oMedianSigmBg,
    RseMedianSigmBg =oRseMedianSigmBg,
    MeanSignal = oMeanSignal,
    MedianSignal = oMedianSignal,
    StdSignal = oStdSignal,
    SumSignal = oSumSignal,
    Rse_Signal = oRse_Signal,
    MeanBackground = oMeanBackground,
    MedianBackground = oMedianBackground,
    StdBackground = oStdBackground,
    SumBackground = oSumBackground,
    RseBackground = oRseBackground,
    SignalSaturation = oSignalSaturation,
    FracIgnored = oFracIgnored,
    Diameter = oDiameter,
    PosOffset = oPosOffset,
    EmptySpot = oEmptySpot,
    BadSpot = oBadSpot,
    ReplacedSpot = oReplacedSpot
  )
  
  return(outFrame)
}

# =====================
# MAIN OPERATOR CODE
# =====================

ctx = tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 
#if (length(ctx$labels) == 0) stop("Label factor containing the image name must be defined") 

# Set LD_LIBRARY_PATH environment variable to speed calling pamsoft_grid multiple times
MCR_PATH <- "/opt/mcr/v99"
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
# ---------------------------------------
# END MCR Path setting

docId     <- unique( ctx %>% cselect(documentId)  )[1]
docId     <- docId$documentId


imgInfo   <- prep_image_folder(docId)
props     <- get_operator_props(ctx, imgInfo[1])


# GET necessary columns with their namespace
colNames  <- names(ctx$cselect())


ctx$select() %>% 
  group_by(ds1.grdImageNameUsed)   %>%
  do(do.quant(., props, docId, imgInfo)) %>%
  ctx$addNamespace() %>%
  ctx$save() 

