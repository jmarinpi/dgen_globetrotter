var SliderView = Backbone.View.extend({
  
  el: $('#slider'),

  events: {
    "slide": "slideAction"
  },

  initialize: function () {

    this.render();
    this.model.on('change:sliderValue', this.updateslider, this);
  },

  slideAction: function (event, ui) {
    var self = this;
    
    self.model.set("sliderValue", ui.value);
    self.model.set("valueVarChanged", false);
  },

  updateslider: function () {
    var self = this;
    $(self.el).slider("option", "value", self.model.get("sliderValue"));
  },

  render: function () {
    var self = this;

    if (timeSeriesArray.length == 1) {
      $(self.el).remove();
    } else {

      $(this.el).slider({
        max: self.model.get("sliderMax"),
        min: self.model.get("sliderMin"),
        value: self.model.get("sliderValue"),
        step: self.model.get("sliderStep"),
        animate: "fast"
      });
    }

    $(this.el).width(200);



    return this;
  }
});

var SliderDisplayView = Backbone.View.extend({

  el: $('#slider-display'),

  sliderValue: null,

  initialize: function () {
    this.model.on('change:sliderValue', this.setvalue, this);
    this.render();
  },

  setvalue: function () {
    var self = this;
    $('#slider-value').html(self.model.get("sliderValue"));
  },

  render: function () {
    var self = this;

    var source = $('#slider-view-template').html();
    var template = Handlebars.compile(source);
    var html = template(self.model.toJSON());
    $(self.el).append(html);

  }

});
