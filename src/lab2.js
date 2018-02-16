// import the data from nyse_a.csv using mongoimport command
// mongoimport --db nysedb --collection stocks --type csv --headerline --file

var map = function () {
	emit(this.stock_symbol, this.stock_price_open)
}

var reduce = function (key, value) {
	var maxPrice = 0;
	values.forEach(function(v){
		if (v > maxPrice) maxPrice = v;
	});
	
	return maxPrice;
}

// db.stocks.mapReduce(map, reduce, {out:"mr_maxprice_stocks"})