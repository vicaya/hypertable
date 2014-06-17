var RRDGraph = new Class({
    Implements: [Options, Events],
    options: {
        width: 900,
        leftEdge: 100,
        topEdge: 10,
        gridWidth: 670,
        gridHeight: 200,
        gridBorderColour: '#ccc',
        shade: false,
        secureJSON: false,
        httpMethod: 'get',
        axis: "0 0 1 1"
    },
    initialize: function(element) {
        this.parentElement = element;
        this.buildHeader();
        this.buildContainer();
        this.buildContainers();
        //this.getData(); // calls graphData

    },
    dataURL: function() {
        url = ['data', this.options.type,this.options.stat,this.options.time_interval]
        return url.join('/')
    },

    buildHeader: function() {
        header = $chk(this.options.name) ? this.options.name : this.options.plugin
        this.graphHeader = new Element('h3', {
            'class': 'graph-title',
            'html': header
        });
        $(this.parentElement).grab(this.graphHeader);
    },

    buildContainer: function() {
        $(this.parentElement).set('style', 'padding-top: 1em');

        this.graphContainer = new Element('div', {
            'class': 'graph-rrd container',
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
