var SelectView = Backbone.View.extend({

	el: $('#valVar'),

	events: {
		'selectmenuchange': 'changeValVar'
	},

	initialize: function () {

		this.render();
	},

	changeValVar: function (event, ui) {
		var self = this;


		self.model.set("valueVarChanged", true);

	    var valNameIndex = indexOfObject(globalDefaultParams.valueVariables, "valueVariable", ui.item.value);
	    self.model.set("valueVarUnitsCurrent", globalDefaultParams.valueVariables[valNameIndex].units);
	    self.model.set("valueVarNameCurrent", globalDefaultParams.valueVariables[valNameIndex].labelName);
		self.model.set("valueVarCurrent", ui.item.value);
		
	},

	render: function () {
		var self = this;

		if (globalDefaultParams.valueVariables.length == 1) {
			$(self.el).remove();
			$('#controls').css("margin-left", 60);
			$('#slider-display').css("margin-right", 70);

	 	} else {

			$(this.el).selectmenu({style: 'dropdown'}, { position: { my : "left bottom", at: "left top"}});
			var source = $('#dropdown-template').html();
			var template = Handlebars.compile(source);
			var html = template(this.model.get("valueVariables"));
			$(this.el).html(html);
			$(this.el).val(self.model.get("valueVarCurrent"));
			$(this.el).width(170);
			$(this.el).selectmenu('refresh', true);
		}
	}
});