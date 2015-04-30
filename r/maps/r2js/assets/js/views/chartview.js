

var ChartView = Backbone.View.extend({
	
	el: $('#chart1'),

	chartConfig: chartDefaultParams,

	initialize: function () {
		this.prepdata();
		this.model.on('change:clicked', this.addremoveseries, this);
		this.model.on('change:valueVarCurrent', this.prepdata, this);
		this.model.on('change:chartData', this.changedata, this);
		this.render();
	},

	prepdata: function () {
		var self = this;
		
		var chartDataUpdate = [];
		var valNameIndex = indexOfObject(globalDefaultParams.valueVariables, "valueVariable", self.model.get("valueVarCurrent"));
		
	    for (unit in self.model.get("geogUnitsCharted")) {
	    	var unitName = self.model.get("geogUnitsCharted")[unit];
	    	var newIndex = indexOfObject(geogSeriesArray, "unitName", unitName);
		    
		    var chartDataSeries = {
		    	data: geogSeriesArray[newIndex][self.model.get("valueVarCurrent")],
		    	name: unitName,
				id: unitName,
				events: {
	        		click: function(event) {
	        			self.model.set("sliderValue", event.point.name);
	        		}
	        	} 
		    };
			
		    chartDataUpdate.push(chartDataSeries);
			
		}
		
		self.model.set("yAxisMin", globalDefaultParams.chart1FixedYAxis ? globalDefaultParams.valueVariables[valNameIndex].minValue : null);
		self.model.set("yAxisMax", globalDefaultParams.chart1FixedYAxis ? globalDefaultParams.valueVariables[valNameIndex].maxValue : null);
		self.model.set("chartData", chartDataUpdate);
		

	},

	addremoveseries: function () {

		if (timeSeriesArray.length == 1) {

		} else {

			var self = this;
			if (self.model.get("clicked")) {
				if ($.inArray(self.model.get("geogUnitCurrent"), self.model.get("geogUnitsCharted")) !== -1) {
					$(self.el).highcharts().get(self.model.get("geogUnitCurrent")).remove();
					
					var geogUnitsUpdate = self.model.get("geogUnitsCharted");
					geogUnitsUpdate.splice(geogUnitsUpdate.indexOf(self.model.get("geogUnitCurrent")), 1);
					self.model.set("geogUnitsCharted", geogUnitsUpdate);

					self.model.set("clicked", false);

				} else {
					var newIndex = indexOfObject(geogSeriesArray, "unitName", self.model.get("geogUnitCurrent"));
					$(self.el).highcharts().addSeries({
						data: geogSeriesArray[newIndex][self.model.get("valueVarCurrent")],
						name: self.model.get("geogUnitCurrent"),
						id: self.model.get("geogUnitCurrent"),
						events: {
			        		click: function(event) {
			        			self.model.set("sliderValue", event.point.name);
			        		}
			        	},

					});

					var geogUnitsUpdate = self.model.get("geogUnitsCharted");
					geogUnitsUpdate.push(self.model.get("geogUnitCurrent"));
					self.model.set("geogUnitsCharted", geogUnitsUpdate);

					self.model.set("clicked", false);
				}
			}
		}
	},

	changedata: function () {
		var self = this;
		if (timeSeriesArray.length == 1) {

		} else {
			if (self.model.get("valueVarChanged") == true) {
				var valVarIndex = indexOfObject(globalDefaultParams.valueVariables, "valueVariable", self.model.get("valueVarCurrent"));
				var newColor;
				var colorAxis = globalDefaultParams.valueVariables[valVarIndex].colorAxis;
				if (colorAxis.dataClasses) {
					var dataClasses = colorAxis.dataClasses;
					newColor = dataClasses[0].color;
				} else {
					newColor = colorAxis.maxColor;
				}
				var legendUnits = (self.model.get("valueVarUnitsCurrent") == '') ? '' : ' (' + self.model.get("valueVarUnitsCurrent") + ')';

				var newOptions = {
					series: self.model.get("chartData"),
					colors: [newColor],
					yAxis: {
						min: globalDefaultParams.chart1FixedYAxis ? self.model.get("yAxisMin") : 
							typeof chartParams.yAxis == 'undefined' ? null : 
							typeof chartParams.yAxis.min != 'undefined' ? chartParams.yAxis.min : null,
						max: globalDefaultParams.chart1FixedYAxis ? self.model.get("yAxisMax") : null,
						title: {text: self.model.get("valueVarNameCurrent") + legendUnits}
					}
				};
				
				var oldOptions = $(self.el).highcharts().options;
				oldOptions.series = null;

				$.extend(true, newOptions, chartParams);
				$.extend(true, oldOptions, newOptions);
				
				$(self.el).highcharts().destroy();
				$(self.el).highcharts(oldOptions);


			} else {
				$(self.el).highcharts().series[0].update({
					name: self.model.get("geogUnitCurrent"),
					data: self.model.get("chartData")
				});
			}
		}
	},


	render: function () {
		var self = this;

		if (timeSeriesArray.length == 1) {
			$(self.el).remove();
			$('#chart2').css("height", 637);
			$('#chart2').css("margin-top", 0);
			$('#controls').css("margin-left", 100);
			$('#slider-display').css("margin-left", 50);
		} else {

			
			var valVarIndex = indexOfObject(globalDefaultParams.valueVariables, 'valueVariable', self.model.get("valueVarCurrent"));
			var colorAxis = globalDefaultParams.valueVariables[valVarIndex].colorAxis;
				if (colorAxis.dataClasses) {
					var dataClasses = colorAxis.dataClasses;
					newColor = dataClasses[1].color;
				} else {
					newColor = colorAxis.maxColor;
				}
			var legendUnits = (self.model.get("valueVarUnitsCurrent") == '') ? '' : ' (' + self.model.get("valueVarUnitsCurrent") + ')';


			var configUpdate = self.chartConfig;
			var newConfig = {
				series: self.model.get("chartData"),
					
				yAxis: {
					title: {text: self.model.get("valueVarNameCurrent") + legendUnits},
					min: typeof chartParams.yAxis == 'undefined' ? (globalDefaultParams.chart1FixedYAxis ? self.model.get("yAxisMin") : null) :
						typeof chart1Params.yAxis.min != 'undefined' ? chartParams.yAxis.min : null,
					max: globalDefaultParams.chart1FixedYAxis ? self.model.get("yAxisMax") : null,
				},
				xAxis: {
					title: {text: globalDefaultParams.timeUnitObject.timeUnitLabel},
					min: self.model.get("sliderMin"),
					max: self.model.get("sliderMax")
				},
				colors: [newColor],
				tooltip: {
		          formatter: function () {
		          	if (self.model.get("valueVarUnitsCurrent") == '$') {
		          		return globalDefaultParams.timeUnitObject.timeUnitLabel + ': ' + this.point.name + '<br>' + self.model.get("valueVarNameCurrent") + 
			          	': ' + self.model.get("valueVarUnitsCurrent") + this.point.y.toFixed(globalDefaultParams.dataPrecision) + '<br>' + 
			          	globalDefaultParams.geogUnitObject.geogUnitLabel + ': ' + this.series.name;
		          	} else if (self.model.get("valueVarUnitsCurrent") == '%') {
		          		return globalDefaultParams.timeUnitObject.timeUnitLabel + ': ' + this.point.name + '<br>' + self.model.get("valueVarNameCurrent") + 
			          	': ' + this.point.y.toFixed(globalDefaultParams.dataPrecision) + self.model.get("valueVarUnitsCurrent") + '<br>' + 
			          	globalDefaultParams.geogUnitObject.geogUnitLabel + ': ' + this.series.name;
		          	} else {
			          	return globalDefaultParams.timeUnitObject.timeUnitLabel + ': ' + this.point.name + '<br>' + self.model.get("valueVarNameCurrent") + 
			          	': ' + this.point.y.toFixed(globalDefaultParams.dataPrecision) + ' ' + self.model.get("valueVarUnitsCurrent") + '<br>' + 
			          	globalDefaultParams.geogUnitObject.geogUnitLabel + ': ' + this.series.name;
			        }
		          },
		          headerFormat: ''
		        },
		        plotOptions: {
		        	series: {
		        		marker: {
		        			radius: 4
		        		}
		        	}
		        },
		        lang: {
		        	noData: 'Click a state in the map <br/>or column chart to add.'
		        }		
		    }

			$.extend(true, configUpdate, newConfig);
			self.chartConfig = configUpdate;

			$(self.el).highcharts(self.chartConfig);
		}
	}
});

var ChartView2 = Backbone.View.extend({
	
	el: $('#chart2'),

	chartConfig: chart2DefaultParams,

	initialize: function () {
		this.prepdata();
		this.model.on('change:sliderValue', this.prepdata, this);
		this.model.on('change:chartData2', this.changedata, this);
		this.model.on('change:valueVarCurrent', this.prepdata, this);
		this.render();
	},

	prepdata: function () {
	    var self = this;

	    var valNameIndex = indexOfObject(globalDefaultParams.valueVariables, "valueVariable", self.model.get("valueVarCurrent"));
	    var colorAxis = globalDefaultParams.valueVariables[valNameIndex].colorAxis.dataClasses;
	    var currentTimeIndex = indexOfObject(timeSeriesArray, "timeStep", self.model.get("sliderValue"));

		self.model.set("yAxisMin", globalDefaultParams.valueVariables[valNameIndex].minValue);
		self.model.set("yAxisMax", globalDefaultParams.valueVariables[valNameIndex].maxValue);
		self.model.set("chartData2", timeSeriesArray[currentTimeIndex][self.model.get("valueVarCurrent")]);
	    
	  },

	changedata: function () {
		var self = this;
		if (self.model.get("valueVarChanged") == true) {
			var valVarIndex = indexOfObject(globalDefaultParams.valueVariables, 'valueVariable', self.model.get("valueVarCurrent"));
			var colorAxis = globalDefaultParams.valueVariables[valVarIndex].colorAxis;
			if (colorAxis.dataClasses) {
				var dataClasses = colorAxis.dataClasses;
				newColor = dataClasses[dataClasses.length-1].color;
			} else {
				newColor = [colorAxis.maxColor];
			}
			var legendUnits = (self.model.get("valueVarUnitsCurrent") == '') ? '' : ' (' + self.model.get("valueVarUnitsCurrent") + ')';

			var newOptions = {
				series: [{
					name: self.model.get("geogUnitCurrent"),
					data: self.model.get("chartData2"),
				}],
				colors: newColor,
				yAxis: {
					min: globalDefaultParams.chart2FixedYAxis ? self.model.get("yAxisMin") : 
							typeof chart2Params.yAxis == 'undefined' ? null : 
							typeof chart2Params.yAxis.min != 'undefined' ? chart2Params.yAxis.min : null,
					max: globalDefaultParams.chart2FixedYAxis ? self.model.get("yAxisMax") : null,
					title: {text: self.model.get("valueVarNameCurrent") + legendUnits}
				}
			};
			
			var oldOptions = $(self.el).highcharts().options;

			$.extend(true, newOptions, chart2Params);
			$.extend(true, oldOptions, newOptions);
			
			$(self.el).highcharts().destroy();
			$(self.el).highcharts(oldOptions);

			self.model.set("valueVarChanged", false);
		} else {
			$(self.el).highcharts().series[0].update({data: self.model.get("chartData2")});
		}

	},

	render: function () {

		var self = this;
		var valVarIndex = indexOfObject(globalDefaultParams.valueVariables, 'valueVariable', self.model.get("valueVarCurrent"));
		var colorAxis = globalDefaultParams.valueVariables[valVarIndex].colorAxis;
		if (colorAxis.dataClasses) {
			var newColor = [];
		} else {
			var newColor = [colorAxis.maxColor];
		}

		var legendUnits = (self.model.get("valueVarUnitsCurrent") == '') ? '' : ' (' + self.model.get("valueVarUnitsCurrent") + ')';

		var configUpdate = self.chartConfig;

		var newConfig = {
			series: [{
				data: self.model.get("chartData2"),
				name: self.model.get("sliderValue"),
				events: {
					click: function (event) {
						if (timeSeriesArray.length !== 1) {
							self.model.set("geogUnitCurrent", event.point.geogUnit);
							self.model.set("clicked", true);
						} else {

						}
					}
				}
				
			}],
			yAxis: {
				min: typeof chart2Params.yAxis == 'undefined' ? (globalDefaultParams.chart2FixedYAxis ? self.model.get("yAxisMin") : null) :
						typeof chart2Params.yAxis.min != 'undefined' ? chart2Params.yAxis.min : null,
				max: globalDefaultParams.chart2FixedYAxis ? self.model.get("yAxisMax") : null,
				title: {text: self.model.get("valueVarNameCurrent") + legendUnits}
			},
			xAxis: {
				min: 0,
				max: geogSeriesArray.length-1,
				title: {text: globalDefaultParams.geogUnitObject.geogUnitLabel},
				type: 'category',
				labels: {
					enabled: true
				}
			},
			colors: newColor,
			tooltip: {
	          formatter: function () {
	          	if (self.model.get("valueVarUnitsCurrent") == '$') {
	          		return globalDefaultParams.timeUnitObject.timeUnitLabel + ': ' + this.point.Year + '<br>' + self.model.get("valueVarNameCurrent") + 
		          	': ' + self.model.get("valueVarUnitsCurrent") + this.point.value.toFixed(globalDefaultParams.dataPrecision) + '<br>' + 
		          	globalDefaultParams.geogUnitObject.geogUnitLabel + ': ' + this.point.geogUnit;
	          	} else if (self.model.get("valueVarUnitsCurrent") == '%') {
	          		return globalDefaultParams.timeUnitObject.timeUnitLabel + ': ' + this.point.Year + '<br>' + self.model.get("valueVarNameCurrent") + 
		          	': ' + this.point.value.toFixed(globalDefaultParams.dataPrecision) + self.model.get("valueVarUnitsCurrent") + '<br>' + 
		          	globalDefaultParams.geogUnitObject.geogUnitLabel + ': ' + this.point.geogUnit;
	          	} else {
		          	return globalDefaultParams.timeUnitObject.timeUnitLabel + ': ' + this.point.Year + '<br>' + self.model.get("valueVarNameCurrent") + 
		          	': ' + this.point.value.toFixed(globalDefaultParams.dataPrecision) + ' ' + self.model.get("valueVarUnitsCurrent") + '<br>' + 
		          	globalDefaultParams.geogUnitObject.geogUnitLabel + ': ' + this.point.geogUnit;
			    }
	          },
	          headerFormat: ''
	        }

		};

		$.extend(true, configUpdate, newConfig);

		self.chartConfig = configUpdate;

		console.dir(self.chartConfig.yAxis.min);

		$(self.el).highcharts(self.chartConfig);

	}
});
