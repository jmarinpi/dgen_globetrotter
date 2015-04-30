var MapView = Backbone.View.extend({

  el: $('#map'),

  mapConfig: mapDefaultParams,


  initialize: function () {

    this.prepdata();
    this.model.on('change:sliderValue', this.prepdata, this);
    this.model.on('change:valueVarCurrent', this.prepdata, this);
    this.model.on('change:mapData', this.changedata, this);
    this.model.on('change:valueVarNameCurrent', this.changename, this);
    this.model.on('change:clicked', this.highlight, this);
    
    this.render();
  },

  prepdata: function () {
    var self = this;
    var currentTimeIndex = indexOfObject(timeSeriesArray, "timeStep", self.model.get("sliderValue"));
    var valNameIndex = indexOfObject(globalDefaultParams.valueVariables, "valueVariable", self.model.get("valueVarCurrent"));
    self.model.set("colorAxisCurrent", globalDefaultParams.valueVariables[valNameIndex].colorAxis);
    self.model.set("mapData", timeSeriesArray[currentTimeIndex][self.model.get("valueVarCurrent")]);
  },

  changedata: function () {
    var self = this;

    var legendUnits = (self.model.get("valueVarUnitsCurrent") == '') ? '' : ' (' + self.model.get("valueVarUnitsCurrent") + ')';
      
    var newOptions = {
      series: [{
        animation: false,
        data: self.model.get("mapData")
      }],
      colorAxis: self.model.get("colorAxisCurrent"),
      chart: {
        redraw: false
      }
    };

    var oldOptions = $(self.el).highcharts().options;

    var legend = {
      title: {text: self.model.get("valueVarNameCurrent") + legendUnits}
    };

    $.extend(true, oldOptions.legend, legend);

    $.extend(true, oldOptions, newOptions);

    $(self.el).highcharts().destroy();
    map1 = $(self.el).highcharts('Map', oldOptions, false);

    for (unit in self.model.get("geogUnitsCharted")) {
      $(self.el).highcharts().get(self.model.get("geogUnitsCharted")[unit]).select(true, true);
    }

  },

  changename: function () {
    var self = this;

    var newName = {
      series: [{
        name: self.model.get("valueVarNameCurrent")
      }],
      chart: {
        redraw: true
      }
    };

    var nameUpdate = $(self.el).highcharts().options;

    $.extend(true, nameUpdate, newName);

    map1 = $(self.el).highcharts('Map', nameUpdate);

  },

  highlight: function () {
    var self = this;

    if (self.model.get("clicked")) {
      if ($(self.el).highcharts().get(self.model.get("geogUnitCurrent")).selected) {
        $(self.el).highcharts().get(self.model.get("geogUnitCurrent")).select(false, true);
      } else {
        $(self.el).highcharts().get(self.model.get("geogUnitCurrent")).select(true, true);
      }
    }
  },

  render: function () {
    var self = this;

    var legendUnits = (self.model.get("valueVarUnitsCurrent") == '') ? '' : ' (' + self.model.get("valueVarUnitsCurrent") + ')';
    
    function mapReady() {
        var newConfig = {
          series: [{
            animation: {
              duration: 500
            },
            data: self.model.get("mapData"),
            mapData: (typeof geojson !== 'undefined') ? customMapData : Highcharts.maps[mapKey],
            name: self.model.get("valueVarNameCurrent"),
            states: {
              select: {
                color: '#FAFA4B'
              }
            },
          }],

          legend: {
            title: {text: self.model.get("valueVarNameCurrent") + legendUnits}
          },

          plotOptions: {
            series: {
              events: {
                click: function (event) {
                  if (timeSeriesArray.length !== 1) {
                    self.model.set("geogUnitCurrent", event.point[globalDefaultParams.geogUnitObject.geogUnit]);
                    self.model.set("clicked", true);
                  }
                }
              }
            }
          },

          tooltip: {
              formatter: function() {
                if (self.model.get("valueVarUnitsCurrent") == '$') {
                  return globalDefaultParams.timeUnitObject.timeUnitLabel + ': ' + this.point[globalDefaultParams.timeUnitObject.timeUnitLabel] + '<br>' + 
                  self.model.get("valueVarNameCurrent") + ": " + self.model.get("valueVarUnitsCurrent") + this.point.value.toFixed(globalDefaultParams.dataPrecision) +  
                  '<br>' + globalDefaultParams.geogUnitObject.geogUnitLabel + ': ' + this.point.geogUnit;
                } else if (self.model.get("valueVarUnitsCurrent") == '%') {
                  return globalDefaultParams.timeUnitObject.timeUnitLabel + ': ' + this.point[globalDefaultParams.timeUnitObject.timeUnitLabel] + '<br>' + 
                  self.model.get("valueVarNameCurrent") + ": " + this.point.value.toFixed(globalDefaultParams.dataPrecision) + self.model.get("valueVarUnitsCurrent") + 
                  '<br>' + globalDefaultParams.geogUnitObject.geogUnitLabel + ': ' + this.point.geogUnit;
                } else {
                  return globalDefaultParams.timeUnitObject.timeUnitLabel + ': ' + this.point[globalDefaultParams.timeUnitObject.timeUnitLabel] + '<br>' + 
                  self.model.get("valueVarNameCurrent") + ": " + this.point.value.toFixed(globalDefaultParams.dataPrecision) + ' ' + self.model.get("valueVarUnitsCurrent") + 
                  '<br>' + globalDefaultParams.geogUnitObject.geogUnitLabel + ': ' + this.point.geogUnit;
               } 
              },

              headerFormat: ''
            },

          colorAxis: self.model.get("colorAxisCurrent")
        };

        $.extend(true, self.mapConfig, newConfig);
        
        var map = $(self.el).highcharts('Map', self.mapConfig);

        if (timeSeriesArray.length !== 1) {
          for (unit in self.model.get("geogUnitsCharted")) {
            var unitName = self.model.get("geogUnitsCharted")[unit];
            $(self.el).highcharts().get(unitName).select(true, false);
          }
        }

        var joinCount = 0;

        for (feature in $(self.el).highcharts().series[0].mapData) {
          for (property in $(self.el).highcharts().series[0].mapData[feature].properties) {
            if (property === globalDefaultParams.joinField) {
              joinCount += 1;
            }
          }
        }

        if (joinCount == 0) {
          alert("No units joined to data using joinField '" + globalDefaultParams.joinField + "'.\n\n" +
            "'" + globalDefaultParams.joinField + "' is not a property of any geography units in basemap.");
          console.log(joinCount + " units joined to data using joinField '" + globalDefaultParams.joinField + "'");
        } else if (joinCount == 1) {
          console.log(joinCount + " unit joined to data using joinField '" + globalDefaultParams.joinField + "'");
        } else {
          console.log(joinCount + " units joined to data using joinField '" + globalDefaultParams.joinField + "'");
        }
      }

    if (typeof geojson !== 'undefined') {
      var customMapData = Highcharts.geojson(geojson, 'map');

      customMapData.forEach(function(element) {
        if (typeof element.name == 'undefined') {
          element.name = String(element.properties[globalDefaultParams.joinField]);
        }
      });
      mapReady();
      
    } else {
      var mapBasePath = "http://code.highcharts.com/mapdata/";
      var mapKey = Highcharts.mapDataIndex[globalDefaultParams.mapGroup][globalDefaultParams.map];
      var mapPath = mapBasePath + mapKey;
      mapKey = mapKey.slice(0,-3);

      $.getScript(mapPath, function () {
        mapReady();
      });


    }
  }
});