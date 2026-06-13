
library(forbesListR)
library(magrittr)


nlst <- list( "30 Under 30", "Powerful Women",         "Celebrities", "MLB Valuations", "NASCAR Valuations",         "NFL Valuations", "NBA Valuations", "NHL Valuations",         "Soccer Valuations", "Top Colleges", "Top Business Schools",         "Innovative Companies", "Small Companies", "Best Employers",         "Largest Private Companies", "Asia 200", "Asia Fab 50",         "Most Promising Companies", "Powerful Brands", "Growth Companies",         "Best Employers", "Global 2000", "Best Countries for Business",         "Best Cities for Business", "Best States for Business",         "Best Small Cities for Business", "Top VCs", "Athletes",  "Forbes 400", "Billionaires", "Self Made Women",         "Richest in Tech", "Hong Kong 50", "Australia 50", "China 50",         "Taiwan 50", "India 50", "Japan 50", "Africa 50", "Korea 50",         "Malaysia 50", "Philippines 50", "Singapore 50", "Indonesia 50", "Thailand 50", "Richest Families","Powerful People")


year = 2019



for (i in nlst){

    i_nice = gsub(" ",'_',i)

    csv_f = paste("~/DATA/pickle/Forbes/",year,"/",i_nice,"_",year,".csv", sep ="")

    print(csv_f)
     
    a = tryCatch({get_year_forbes_list_data(list = i, year = year)},            
                    error= function(e){print("got error")}
                                )
                
    write.csv(a, csv_f)
     
}




