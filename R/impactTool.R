### relativeProductImpact
#' Relative Product Impact Rank
#'
#' @description The function relativeProductImpact() takes a vector of comma-separated product codes and returns each product's relative impact -- basically a product's conversion weighted by its daily TTV.
#'
#' @param productCodes vector (length == 1) of comma-separated product_codes
#' @param dateStart date from which kpi's start being measured format yyyy-mm-dd
#' @param dateEnd date to which kpi's stop being measured format yyyy-mm-dd
#' @param connections creates group of connections for multiple uses
#'
#' @return data frame containing product_code, family, days_live, page_views, ttv, num_checkouts, conversion, ttv_per_day, impactScore, impactScoreNorm, and impact
#'
#' @import DBI
#' @import RPostgreSQL
#' @import data.table
#'
#' @examples
#'
#' @author Lee Stirling, Kathryn Harris 2016-08-24
#'
#' @export
relativeProductImpact <- function(productCodes, dateStart, dateEnd, connections) {

### get products
query_prod <- paste0('
              SELECT product_code, family, current_date-date(published_date) as days_live
              FROM product
              WHERE product_code IN (', productCodes, ')
                AND published_date < CURRENT_DATE
                AND published_date IS NOT NULL
              ')

products <- DBI::dbGetQuery(poolNames, query_prod)

### get page views for products
query_views <- paste0('
                SELECT product_code, sum(number_of_views) as page_views
                FROM noths.product_page_views_by_date
                WHERE date BETWEEN \'', as.character(dateStart), '\'
                  AND \'', as.character(dateEnd), '\'
                  AND product_code IN (', productCodes, ')
                GROUP BY product_code
                      ')

views <- DBI::dbGetQuery(poolNames,query_views)

### get ttv and checkouts for products
query_trans <- paste0('
                SELECT product_code, sum(ttv) as TTV, count(distinct checkout_id) as num_checkouts
                FROM transaction_line
                WHERE date BETWEEN \'', as.character(dateStart), '\'
                  AND \'', as.character(dateEnd), '\'
                  AND product_code IN (', productCodes, ')
                GROUP BY product_code
                      ')

trans <- DBI::dbGetQuery(poolNames,query_trans)

### put products, views and trans together
full_file <- data.frame(
              products,
              page_views=views[match(products$product_code,views$product_code), -1],
              trans[match(products$product_code,trans$product_code), -1]
              )

### get conversion
### use ifelse to susbtitute in 1 for conversion when checkouts > page views
full_file$conversion <- ifelse(full_file$page_views < full_file$num_checkouts, 1, full_file$num_checkouts / full_file$page_views)

### If there are any NAs in {page_views, ttv, num_checkouts, conversion}, replace them with 0. (e.g. if there are no page views or checkouts)
full_file[, c("page_views", "ttv", "num_checkouts", "conversion")] <- lapply(full_file[, c("page_views", "ttv", "num_checkouts", "conversion")], function(x) replace(x, is.na(x), 0))

### substitute in average conversion for products with either: (A) a single checkout (these have an unreliable base for conversion) resulting in > 0.15 conversion or (B) more checkouts than page views
invalid <- which(full_file$conversion > 0.15 & full_file$num_checkouts == 1)
if(length(invalid) == 0){
  full_file <- full_file
  }else{
  full_fileInvalid <- full_file[invalid, ]
  full_fileValid <- full_file[-invalid, ]

  meanConversionByFamily <- sapply(split(full_fileValid, full_fileValid$family), function(x) mean(x$conversion))

  for (i in unique(full_fileValid$family))
    {
      full_fileInvalid[full_fileInvalid$family == i, "conversion"] <- meanConversionByFamily[i]
    }
      full_file <- rbind(full_fileValid, full_fileInvalid)
    }

### get ttv_per_day
full_file$ttv_per_day <- ifelse(full_file$days_live > (as.Date(dateEnd) - as.Date(dateStart)), full_file$ttv / as.numeric(as.Date(dateEnd) - as.Date(dateStart)), full_file$ttv / full_file$days_live)

### get impact metrics
full_file$impactScore <- full_file$conversion * full_file$ttv_per_day
full_file$impactScoreNorm <- (full_file$impactScore - min(full_file$impactScore)) / diff(range(full_file$impactScore))
full_file <- full_file[order(-full_file$impactScore),]
full_file$impact <- 1:nrow(full_file)

### re-order products with 0 checkouts
### used page_views per day to order, with more page views per day being better
### calculation: (page_views + 1) / days_live. +1 added to page_views to control for 0 page views
### this metric may still require work!
### if statement added to catch instances where there might not be any products with zero checkouts
if(!any(full_file$num_checkouts == 0)){
  full_file <- full_file
}else{
  full_file2 <- split(full_file, full_file$num_checkouts == 0); names(full_file2) <- c("checkouts", "noCheckouts")
  full_file2$'noCheckouts' <- data.frame(
    full_file2$'noCheckouts'[
      order((full_file2$'noCheckouts'$page_views + 1) / full_file2$'noCheckouts'$days_live, decreasing = T), -ncol(full_file2$'noCheckouts')],
    "impact" = full_file2$'noCheckouts'$impact)
    full_file <- as.data.frame(data.table::rbindlist(full_file2))
  }

### bit of banter regarding the success of your code
if(nrow(full_file) > 0){
  print(paste("Your query returned impact data for", nrow(full_file), "products"))
}else{
  print("Your query failed to return any impact data. Please check your query")
}

return(full_file)
}
