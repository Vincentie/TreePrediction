# TreePrediction Project

## Project Objective  
  To predict the prices trending of gold futures in SHFE in next 5 trading days according to gold's relevant historical data and sentiments.

## Development Environment
  * Python 3.6.0 :: Anaconda 4.3.1 (64-bit)
  * Jupyter Notebook

## Project Outline
  1. Data Parsing  
    We parse the daily trading data of different gold futures from [SHFE](http://www.shfe.com.cn/data/dailydata/kx/kx20170104.dat) and store them in a table in FData.sqlite Database.  
    We parse the sentimental contexts from [SinaFinance](http://roll.finance.sina.com.cn/finance/gjs/hjzx.shtml) and insert them in a table in Sentiments.sqlite Database.

  2. Data Processing  
    We process trading data and sentiments separately.  
    * Trading data (like HOLC)  
  	    We compute their volume weighted average number for future use.  

    * Sentimental contexts  
  	    We use third-party NLP processing package to perform words partitioning and sentiments grading to serve as emotions and sentiments status of that day.  

  3. Prediction  
  	We choose prediction tree classifier to predict the prices trending of gold futures.  
  	Choose data in trading days from ___01/01/2012___ to ___01/01/2017___ to be training samples, and test the prediction's performance from ___02/21/2017___ to ___04/27/2017___(46 trading days in total).  

## Prediction Performance on testing samples
<table> 
 	<tr> 
 		<th> </th>
 		<th>Precison</th> 
 		<th>Recall</th>
		<th>F1-Score</th>
		<th>Support</th>
	</tr>
	<tr> 
		<th>Up in 5 days</th>
		<th>0.58</th>
		<th>0.32</th> 
		<th>0.41</th> 
		<th>22</th> 
	</tr> 
	<tr> 
		<th>Down in 5 days</th> 
		<th>0.56</th>
		<th>0.79</th> 
		<th>0.66</th> 
		<th>24</th> 
	</tr>
	<tr> 
		<th>avg/total</th> 
		<th>0.57</th>
		<th>0.57</th> 
		<th>0.54</th> 
		<th>46</th> 
	</tr> 
</table>