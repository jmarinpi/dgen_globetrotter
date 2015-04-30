
//get the index of the time step 
function indexOfObject(array, name, value) {
    for (q=0; q<array.length; q++) {
        if (array[q][name] == value) {
            return q;
        }
    }
}

function sortByKey(array, key) {
    return array.sort(function(a, b) {
        var x = a[key]; var y = b[key];
        return ((x < y) ? -1 : ((x > y) ? 1 : 0));
    });
}

function inTimeSeriesArray(array, object) {
	for (i=0; i<array.length; i++) {
		if (array[i].timeStep == object[globalDefaultParams.timeUnitObject.timeUnit]) {
			return true;
			break;
		}
	}
}

Array.min = function( array ){
    return Math.min.apply( Math, array );
};

Array.max = function( array ){
    return Math.max.apply( Math, array );
};


$.extend(true, mapDefaultParams, mapParams);
$.extend(true, chartDefaultParams, chartParams);
$.extend(true, chart2DefaultParams, chart2Params);

var timeSeriesArray = [];
var geogSeriesArray = [];

$(document).ready(function () {

	//create array to hold all time step values
	var timeRange = [];
	//create array to hold all geog unit values
	var geogRange = [];

	//loop through all raw data points to create time Series collection
	for (j=0; j<data_raw.length; j++) {
		
		if (inTimeSeriesArray(timeSeriesArray, data_raw[j])) {
			
			for (i=0; i<globalDefaultParams.valueVariables.length; i++) {
				//sets the value variable and time step we are working with
				var valueVar = globalDefaultParams.valueVariables[i].valueVariable;
				var timeIndex = indexOfObject(timeSeriesArray, "timeStep", data_raw[j][globalDefaultParams.timeUnitObject.timeUnit]);

				var dataPoint = {};
				dataPoint[globalDefaultParams.timeUnitObject.timeUnitLabel] = data_raw[j][globalDefaultParams.timeUnitObject.timeUnit];
				dataPoint.geogUnit = data_raw[j].geogUnit;
				dataPoint.value = data_raw[j][valueVar];
				dataPoint.name = data_raw[j].geogUnit;
				dataPoint.id = data_raw[j].geogUnit;
				dataPoint.y = data_raw[j][valueVar];

				if (globalDefaultParams.valueVariables[i].colorAxis.dataClasses) {
					var dataClasses = globalDefaultParams.valueVariables[i].colorAxis.dataClasses;
					for (dataClass in dataClasses) {
						if (data_raw[j][valueVar] < dataClasses[dataClass].to) {
							dataPoint.color = dataClasses[dataClass].color;
							break;
						}
					}
				}

				timeSeriesArray[timeIndex][valueVar].push(dataPoint);
				timeRange = timeRange.sort();

			}	
		} else {
			
			//put time value into timeRange array
			timeRange.push(data_raw[j][globalDefaultParams.timeUnitObject.timeUnit]);
			
			//create new time series object
			var timeSeries = {timeStep: data_raw[j][globalDefaultParams.timeUnitObject.timeUnit]};

			//swap value variable name for "value" in object and add object to new time series for that value variable 
			for (i=0; i<globalDefaultParams.valueVariables.length; i++) {
				//sets the value variable we are working with
				var valueVar = globalDefaultParams.valueVariables[i].valueVariable;

				var dataPoint = {};
				dataPoint[globalDefaultParams.timeUnitObject.timeUnitLabel] = data_raw[j][globalDefaultParams.timeUnitObject.timeUnit];
				dataPoint.geogUnit = data_raw[j].geogUnit;
				dataPoint.value = data_raw[j][valueVar];
				dataPoint.name = data_raw[j].geogUnit;
				dataPoint.id = data_raw[j].geogUnit;
				dataPoint.y = data_raw[j][valueVar];

				if (globalDefaultParams.valueVariables[i].colorAxis.dataClasses) {

					var dataClasses = globalDefaultParams.valueVariables[i].colorAxis.dataClasses;
					for (dataClass in dataClasses) {
						if (data_raw[j][valueVar] < dataClasses[dataClass].to) {
							dataPoint.color = dataClasses[dataClass].color;
							break;
						}
					}
				}

				timeSeries[valueVar] = [dataPoint];
			}

			timeSeriesArray.push(timeSeries);
			timeRange = timeRange.sort();

		}

		if ($.inArray(data_raw[j].geogUnit, geogRange) !== -1) {
			//get index of the geog unit in the series array
			var geogIndex = indexOfObject(geogSeriesArray, "unitName", data_raw[j].geogUnit);
			for (i=0; i<globalDefaultParams.valueVariables.length; i++) {
				var valueVar = globalDefaultParams.valueVariables[i].valueVariable;
				var dataPoint = {};
				dataPoint.name = data_raw[j][globalDefaultParams.timeUnitObject.timeUnit];
				dataPoint.x = data_raw[j][globalDefaultParams.timeUnitObject.timeUnit];
				dataPoint.y = data_raw[j][valueVar];

				if (globalDefaultParams.valueVariables[i].colorAxis.dataClasses) {
					var datarawIndex = indexOfObject(timeSeriesArray, "timeStep", data_raw[j][globalDefaultParams.timeUnitObject.timeUnit]);
					var timeStep = timeSeriesArray[datarawIndex][valueVar];
					var geogIndex = indexOfObject(timeStep, "geogUnit", data_raw[j].geogUnit);
					dataPoint.color = timeStep[geogIndex].color;
				}

				geogSeriesArray[geogIndex][valueVar].push(dataPoint);
			}
			
		} else {
			//put geographic unit (e.g. state) into geogRange array
			geogRange.push(data_raw[j].geogUnit);

			//create new data 
			var geogSeries = {
				unitName: data_raw[j].geogUnit
			};

			for (i=0; i<globalDefaultParams.valueVariables.length; i++) {
				//sets the value variable we are working with
				var valueVar = globalDefaultParams.valueVariables[i].valueVariable;
				//create new array for each value variable
				var dataPoint = {};
				dataPoint.name = data_raw[j][globalDefaultParams.timeUnitObject.timeUnit];
				dataPoint.x = data_raw[j][globalDefaultParams.timeUnitObject.timeUnit];
				dataPoint.y = data_raw[j][valueVar];
				geogSeries[valueVar] = [dataPoint];

				if (globalDefaultParams.valueVariables[i].colorAxis.dataClasses) {
					var datarawIndex = indexOfObject(timeSeriesArray, "timeStep", data_raw[j][globalDefaultParams.timeUnitObject.timeUnit]);
					var timeStep = timeSeriesArray[datarawIndex][valueVar];
					var geogIndex = indexOfObject(timeStep, "geogUnit", data_raw[j].geogUnit);
					dataPoint.color = timeStep[geogIndex].color;
				}
			}

			geogSeriesArray.push(geogSeries);
		}
	}
	geogSeriesArray = _.sortBy( geogSeriesArray, 'unitName' );
	for (timeStep in timeSeriesArray) {
		for (i in globalDefaultParams.valueVariables) {
			timeSeriesArray[timeStep][globalDefaultParams.valueVariables[i].valueVariable] = _.sortBy(timeSeriesArray[timeStep][globalDefaultParams.valueVariables[i].valueVariable], 'geogUnit');
		}
	}
	timeSeriesArray = _.sortBy(timeSeriesArray, 'timeStep');
	
	$('#main-title').html(globalDefaultParams.title.text);

	this.appModel = new AppModel({
		sliderMin: Array.min(timeRange),
		sliderMax: Array.max(timeRange),
		sliderValue: timeRange[0],
		sliderStep: (timeRange[1] - timeRange[0]),
		geogUnitCurrent: timeSeriesArray.length!==1 ? geogSeriesArray[0].unitName: null,
		geogUnitsCharted: timeSeriesArray.length!==1 ? [geogSeriesArray[0].unitName] : null,
		valueVariables: globalDefaultParams.valueVariables,
		valueVarCurrent: globalDefaultParams.valueVariables[0].valueVariable,
		valueVarNameCurrent: globalDefaultParams.valueVariables[0].labelName,
		valueVarUnitsCurrent: globalDefaultParams.valueVariables[0].units,
		valueChanged: false,
		yAxisMin: globalDefaultParams.valueVariables[0].minValue,
		yAxisMax: globalDefaultParams.valueVariables[0].maxValue,
		timeUnitLabel: globalDefaultParams.timeUnitObject.timeUnitLabel,
		mapOptions: mapDefaultParams,
		colorAxisCurrent: globalDefaultParams.valueVariables[0].colorAxis
	});
 	
 	
 	
	this.selectView = new SelectView({model: this.appModel});

	this.sliderView = new SliderView({model: this.appModel});

	this.sliderValueView = new SliderDisplayView({model: this.appModel});

	this.mapView = new MapView({model: this.appModel});

	this.chartView = new ChartView({model: this.appModel});

	this.chartView2 = new ChartView2({model: this.appModel});

});