var HTMonitoring = new Class({
    Implements: [Options, Events],
    options: {
        width: 900,
        height: 220,
        leftEdge: 100,
        topEdge: 10,
        gridWidth: 670,
        gridHeight: 200,
        columns: 60,
        rows: 8,
        gridBorderColour: '#ccc',
        shade: false,
        secureJSON: false,
        httpMethod: 'get',
        axis: "0 0 1 1"
    },
    initialize: function(element, type, stat,time_interval,options) {

        this.parentElement = element;
        this.setOptions(options);
        this.options.type = type;
        this.options.stat = stat;
        this.options.time_interval = time_interval;
        this.buildGraphHeader();
        this.buildGraphContainer();

        this.buildContainers();
        this.getData(); // calls graphData

    },
    dataURL: function() {
        url = ['data', this.options.type,this.options.stat,this.options.time_interval]
        return url.join('/')
    },
    getData: function() {

        this.request = new Request.JSONP({
            url: this.dataURL(),
            data: this.requestData,
            secure: this.options.secureJSON,
            method: this.options.httpMethod,
            onComplete: function(json) {
                this.graphData(json);
            }.bind(this),
            onFailure: function(header, value) {
                $(this.parentElement).set('html', header)
            }.bind(this),
        });

        this.request.send();
    },

    buildGraphHeader: function() {
        header = $chk(this.options.name) ? this.options.name : this.options.plugin
        this.graphHeader = new Element('h3', {
            'class': 'graph-title',
            'html': header
        });
        $(this.parentElement).grab(this.graphHeader);
    },
    buildGraphContainer: function() {
        $(this.parentElement).set('style', 'padding-top: 1em');

        this.graphContainer = new Element('div', {
            'class': 'graph container',
            'styles': {
                'margin-bottom': '24px'
            }
        });
        $(this.parentElement).grab(this.graphContainer);
    },

    buildErrorContainer:function() {
        this.errorContainer =  new Element('div', {
            'class': 'warning',
        });
        $(this.parentElement).grab(this.errorContainer,'top');
    }

});

var HTMGraph = new Class({
    Extends: HTMonitoring,
    Implements: Chain,
    // assemble data to graph, then draw it
    graphData: function(data) {
        this.ys        = [];
        this.instances = [];
        this.metrics   = [];
        this.keys = new Hash(data["keys"]);
        this.timescales = new Hash(data["time_intervals"]);
        this.type = this.options.type;
        this.stat = this.options.stat;
        this.chart = document.getElementById('chart')
        this.buildSelectors();
        google.setOnLoadCallback(this.drawGoogleChart(data));
        if (data['graph']['error'] && data['graph']['error']!='') {
            this.displayError(data['graph']['error']);
        }
    },

    drawGoogleChart: function(gdata) {
        //console.log("graph data");
        //console.log(gdata);

        var gtable = new google.visualization.DataTable();
        gtable.addColumn('string', 'Tables');
        for( stat in gdata['graph']['stats']) {
            gtable.addColumn('number',gdata['graph']['stats'][stat]);
        }
        gtable.addRows(gdata['graph']['size']);
        column = 0;
        for( key in gdata['graph']['data']) {
            row=0;
            gtable.setValue(column,row,key);
            for (row=0;row<gdata['graph']['data'][key].length;row++) {
                gtable.setValue(column,row+1,gdata['graph']['data'][key][row]);
            }
            column++;
        }

        chart = new google.visualization.BarChart(document.getElementById('chart'));
        chart.draw(gtable, {width: 900, height: 300,
                            vAxis: {title: gdata['graph']['vaxis']['title'],titleColor: '#73d216'} ,
                            hAxis: {title: gdata['graph']['haxis']['title'], titleColor: '#c17d11'},
                           colors: gdata['graph']['colors'],
                           title: gdata['graph']['title'],

                         });
    },

    displayError: function(error) {
        this.buildErrorContainer();
        this.errorContainer.set('text',error);
    },

    buildContainers: function() {

        this.selectContainer = new Element('div', {
            'class': 'timescale container',
            'styles': {
                'float': 'right',
                'width': '700px',
                'margin': '5px'
            }
        });
        $(this.parentElement).grab(this.selectContainer, 'top')


        this.statContainer = new Element('div',{
            'class': 'stat container',
            'styles': {
                'float': 'left',
                'width': '200px',
            }
        });

        this.timeContainer = new Element('div',{
            'class': 'time container',
            'styles': {
                'float': 'left',
                'width': '100px',
            }
        });

        this.submitContainer = new Element('div',{
            'class': 'submit container',
            'styles': {
                'float': 'left',
                'width': '100px',
            }
        });

    },


    buildSelectors: function() {
        var container = $(this.selectContainer);
        var stat_container = $(this.statContainer);
        var time_container = $(this.timeContainer);
        var submit_container = $(this.submitContainer);
        var form = new Element('form', {
            'action': this.dataURL(),
            'method': 'get',
            'events': {
                'submit': function(e, foo) {
                    e.stop();
                    this.options.stat = $('stat').get('value');
                    this.options.time_interval = $('time_interval').get('value');
                    this.chart.innerHTML= '';
                    $(this.timeContainer).empty();
                    $(this.statContainer).empty();
                    $(this.selectContainer).empty();
                    if (this.errorContainer) {
                        $(this.errorContainer).dispose();
                    }
                    /* Draw everything again. */
                    this.getData();
                }.bind(this)
            }
        });

        var stat_select = new Element('select', { 'id': 'stat', 'class': 'date timescale' });
        var selected_stat = this.options.stat;

        this.keys.each(function(name,stat) {
            var html = '{stat}'.substitute({'stat': name });
            var option = new Element('option', {
                html: html,
                value: stat,
                selected: (stat == selected_stat ? 'selected' : '')

            });
            stat_select.grab(option)
        });

        var time_select = new Element('select', { 'id':'time_interval','class': 'date timescale' });
        var selected_time = this.options.time_interval;

        this.timescales.each(function(label,hour) {
            var option = new Element('option', {
                html: label,
                value: hour,
                selected: (hour == selected_time ? 'selected' : '')

            });
            time_select.grab(option)
        });
        var submit = new Element('input', { 'type': 'submit', 'value': 'show' });
        stat_container.grab(stat_select);
        time_container.grab(time_select);
        submit_container.grab(submit);
        form.grab(stat_container);
        form.grab(time_container);
        form.grab(submit_container);
        container.grab(form);
    },
});
