#install.packages("sde")
require("sde")
#install.packages("xts")
library(xts)
alldata<-read.csv("D:/resampled_price.csv")
alldata$time <- strptime(x = as.character(alldata$time),
                                format = "%Y-%m-%d %H:%M:%S")
alldata<-na.omit(alldata)
mid<-(alldata['bid']+alldata['ask'])/2
start<-580000
mid<-mid[start:699842,]
df_ts <- ts(data=mid,start=start,end=699842,frequency = 1)

spread<-mid['bid']
ou.lik <- function(x) {
  function(theta1,theta2,theta3) {
    n <- length(x)
    dt <- deltat(x)
    -sum(dcOU(x=x[2:n], Dt=dt, x0=x[1:(n-1)],
              theta=c(theta1,theta2,theta3), log=TRUE))
  }
}
ou.fit <- mle(ou.lik(df_ts),
              start=list(theta1=0,theta2=0.5,theta3=0.2),
              method="L-BFGS-B",lower=c(0,1e-5,1e-3), upper=c(1,1,1))
ou.coe <- coef(ou.fit)
ou.coe


ou.sim <- function(a,b,c,N,startval){
  return(sde.sim(0,1,startval,N,
                  drift = expression(1.00000000-0.46489810*x),
                  sigma = expression(0.01776727),sigma.x = expression(0)))
  }

output<-matrix(0, 5064, 2000)
for (i in 1:600){
walk<-sde.sim(0,1,2.172,101280,
        drift = expression(1-0.46489810*x),
        sigma = expression(0.01438023 ),sigma.x = expression(0))
output[,i]<-walk[seq(2,101281,20)]
}
write.csv(output, file = "D:/Algooutput2.csv")
