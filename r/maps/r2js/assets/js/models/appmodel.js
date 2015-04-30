var AppModel = Backbone.Model.extend({
  
  defaults: {
    sliderMin: null,
    sliderMax: null,
    sliderValue: null,
    sliderStep: null,
    mapData: [],
    chartData: [],
    chartData2: [],
    geogUnitCurrent: null,
    geogUnitsCharted: [],
    clicked: false,
    valueVariables: [],
    valueVarCurrent: null,
    valueVarNameCurrent: null,
    valueVarUnitsCurrent: null,
    valueVarChanged: false,
    yAxisMin: null,
    yAxisMax: null,
    timeUnit: null,
    colorAxisCurrent: {}  
}

});