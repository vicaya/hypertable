Hash.implement({
    sort:function(fn){
        var out = [],
        keysToSort = this.getKeys(),
        m = this.getLength();
        (typeof fn == 'function') ? keysToSort.sort(fn) : keysToSort.sort();
        for (var i=0; i<m; i++){
            var o = {};
            o[keysToSort[i]] = this[keysToSort[i]];
            out.push(o);
        }
        return out;
    }
});

function sortNumber(a,b) {
    return a - b;
}

var HTMonitoring = new Class({
    Implements: [Options, Events],
    options: {
        width: 1000,
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

    initialize: function(element,type,stat,time_interval,resolution,options) {
        this.parentElement = element;
        this.setOptions(options);
        this.options.type = type;
        this.options.stat = stat;
        this.options.time_interval = time_interval;
        this.options.start_time = options.start_time;
        this.options.end_time = options.end_time;
        this.options.sort = 'Name';
        this.options.sort_options = ['Name','Value'];
        this.options.selected_rs = options.selected_rs;
        this.options.resolution = resolution;
        this.chart = '';
        this.buildContainers();
        this.getData(); // calls graphData
    },



    dataURL: function() {
       url = ['data', this.options.type]
        if (this.options.stat) {
            url.push(this.options.stat.replace("/","_"))
	}
        if(this.options.time_interval && this.options.stat != 'servers')
            url.push(this.options.time_interval)
        if(this.options.start_time && this.options.stat != 'servers')
            url.push(this.parseGraphDateSecs(this.options.start_time))
        if(this.options.end_time && this.options.stat != 'servers')
            url.push(this.parseGraphDateSecs(this.options.end_time))
        if(this.options.sort)
            url.push(this.options.sort)
        if (this.options.resolution)
            url.push(this.options.resolution);
        return url.join('/')
    },

    getData: function() {
        this.request = new Request.JSONP({
            url: this.dataURL(),
            data: this.requestData,
            secure: this.options.secureJSON,
            method: this.options.httpMethod,
            onComplete: function(json) {
                if (this.ajaxImageContainer) {
                    $(this.ajaxImageContainer).dispose();
                }
                this.graphData(json);
            }.bind(this),
            onFailure: function(header, value) {
                if (this.ajaxImageContainer) {
                    $(this.ajaxImageContainer).dispose();
                }
                $(this.parentElement).set('html', header)
            }.bind(this),
        });
        this.buildAjaxLoadImageContainer();
        this.request.send();
    },

    buildGraphHeader: function() {
        header = $chk(this.options.name) ? this.options.name : this.options.plugin
        this.graphHeader = new Element('h3', {
            'class': 'graph-title',
            'align': 'center',
            'html': header
        });
        $(this.graphContainer).grab(this.graphHeader);
    },

    buildGraphContainer: function() {
        $(this.parentElement).set('style', 'padding-top: 1em');
        this.graphContainer = new Element('div', {
            'class': 'graph container',
            'id' : 'graph-container',
            'styles': {
                'margin-bottom': '24px',
                'text-align' : 'center'
            }
        });
        $(this.parentElement).grab(this.graphContainer);
    },

    buildGraphImageContainer: function(divid) {
        this.graphImageContainer = new Element('div', {
            'class': 'graph image container',
            'id' : divid,
            'styles': {
                'margin-bottom': '24x'
            }
        });
    },

    buildAjaxLoadImageContainer: function() {
        ajaxImage = new Element('img',{'src':'/images/ajax-loader.gif'});
        this.ajaxImageContainer = new Element('div', {
            'class': 'graph ajax image container',
            'align': 'center',
            'styles': {
                'margin-bottom': '24x'
            }
        });
        $(this.ajaxImageContainer).grab(ajaxImage);
        $(this.parentElement).grab(this.ajaxImageContainer);
    },

    buildErrorContainer:function() {
        this.errorContainer =  new Element('div', {
            'class': 'warning',
        });
        $(this.parentElement).grab(this.errorContainer,'top');
    },


});


var RSGraph = new Class({
    Extends: HTMonitoring,
    Implements: Chain,
    // assemble data to graph, then draw it
    graphData: function(data) {
        this.ys        = [];
        this.instances = [];
        this.metrics   = [];
        this.buildGraphContainer();
        this.buildGraphHeader();
        this.data = data;
        if (this.options.selected_rs != "") {
            this.options.stat = this.options.selected_rs;
        }
        if(this.data['servers']) {
            this.servers = new Hash(this.data["servers"]);
            this.buildSelectors();
            this.displayDefaultGraphs();
        } else if (this.data['graph'] && this.data['graph']['error'] && this.data['graph']['error']!='') {
            this.displayError(this.data['graph']['error']);
        }

    },

    displayError: function(error) {
        if (this.errorContainer) {
            $(this.errorContainer).dispose();
        }
        this.buildErrorContainer();
        this.errorContainer.set('text',error);
    },

    // this method is to show the default 1hour graphs for the first server or selected server in the list
    displayDefaultGraphs: function() {
        this.options.start_time = $('start_time').get('value');
        this.options.end_time = $('end_time').get('value');

        if (this.options.selected_rs != "") {
            this.options.selected_rs = this.options.stat;
        } else {
            this.options.stat = $('rs').get('value');
        }

        if (this.validate(this.options.start_time , this.options.end_time)) {
            if (this.options.type  && this.options.stat ) {
                this.displayGraphInfo();
            }
        }
    },
    
    parseGraphDate: function(date) {
            var date_parts = date.split("-");
            var date = Date.parse(date_parts[1]+"/"+date_parts[2]+"/"+date_parts[0]+"  "+date_parts[3]+":"+date_parts[4]);
            return date;
        },
    parseGraphDateSecs: function(date) {
            return this.parseGraphDate(date).getTime()/1000;
        },

    displayGraphInfo: function() {
        this.buildGraphContainer();
        this.buildGraphHeader();
        selected_server = this.servers.get(this.options.stat);
        if ( selected_server != this.options.stat) {
            selected_server = this.options.stat + " (" + selected_server + ")";
        }
        this.graphHeader.set('text',"RRD Graphs for "+selected_server);
        for (i=0; i < this.data['stats'].length; i++) {
            key = this.data['stats'][i];
            this.buildGraphImageContainer();
            url = this.buildGraphImageUrl(key);
            img = new Element('img', {'src':url,'height':360,'width':1000});
            $(this.graphImageContainer).grab(img);
            $(this.graphContainer).grab(this.graphImageContainer);
        }
    },

    buildGraphImageUrl: function(key) {
        url = ['/graph'];
	url.push(this.options.type);
	url.push(this.options.stat.replace("/","_"))
        url.push(key);
        url.push(this.parseGraphDateSecs(this.options.start_time));
        url.push(this.parseGraphDateSecs(this.options.end_time));
        return url.join('/');
    },

    buildContainers: function() {

        this.rsContainer = new Element('div', {
            'class': 'rs container',
            'styles': {
                'text-align' : 'center',
            }
        });
        $(this.parentElement).grab(this.rsContainer, 'top')

        this.serverContainer = new Element('span',{
            'class': 'server container',
            'styles': {
                'width': '30%',
            }
        });

        this.startContainer = new Element('span',{
            'class': 'start container',
            'styles': {
                'width': '30%',
            }
        });

        this.endContainer = new Element('span',{
            'class': 'end container',
            'styles': {
                'width': '30%',
            }
        });

        this.submitContainer = new Element('span',{
            'class': 'submit container',
            'styles': {
                'width': '10%',
            }
        });

    },

    validate: function(start_date,end_date) {
            start_date = this.parseGraphDateSecs(start_date);
            end_date = this.parseGraphDateSecs(end_date);
        if (start_date == "" || end_date == "") {
            this.displayError("Please provide both start date and end date");
            return false;
        } else if(start_date >= end_date) {
            this.displayError("Start Date should be less than End Date");
            return false;
        }
        return true;
    },

    buildSelectors: function() {
        var rs_container = $(this.rsContainer);
        var start_container = $(this.startContainer);
        var end_container = $(this.endContainer);
        var submit_container = $(this.submitContainer);
        var server_container = $(this.serverContainer);
        var form = new Element('form', {
            'action': '/graphs',
            'method': 'get',
            'events': {
                'submit': function(e, foo) {
                    if (this.validate(this.options.start_time,this.options.end_time)) {
                        if (this.errorContainer) {
                            $(this.errorContainer).dispose();
                        }
                        if (this.graphContainer) {
                            $(this.graphContainer).dispose();
                        }
                        //this.displayGraphInfo();
                    }
                    console.log(e);
                    console.log(foo);
                }.bind(this)
            }
        });

        var rs_select = new Element('select', { 'id': 'rs', 'class': 'rs' ,'name':'server'});
        var selected_rs = this.options.stat;

        this.servers.sort().each(function(element) {
            $each(element, function(name,id) {
                var option = new Element('option', {
                    html: name,
                    value: id,
                    selected: (id == selected_rs ? 'selected' : '')

                });
                rs_select.grab(option)
            });
        });
        
        if (this.options.start_time == "" || this.options.end_time == "") {
            end_date = new Date();
            start_date = end_date.clone().decrement('second',3600);
        } else {
            end_date = this.parseGraphDate(this.options.end_time);
            start_date = this.parseGraphDate(this.options.start_time);
        }

	var rs_label;
	if (this.options.type == "table") 
            rs_label = new Element('label', {'for':'rs','text':'Tables: '});
	else
	    rs_label = new Element('label', {'for':'rs','text':'RangeServers: '});
        var starttime_label = new Element('label', {'for':'start_time','text':'Start Time: '});
        var endtime_label = new Element('label', {'for':'end_time','text':'End Time: '});

        var starttime_select = new Element('input', { 'name':'start_time','id':'start_time','class': 'date start_time','type':'text','value':start_date.format("%Y-%m-%d-%H-%M")});
        var endtime_select = new Element('input' , { 'name':'end_time', 'id':'end_time','class':'date end_time','type':'text','value':end_date.format("%Y-%m-%d-%H-%M")});
        var type_select = new Element('input', {'name':'type','id':'type','type':'hidden','value':this.options.type});
        var submit = new Element('input', { 'type': 'submit', 'value': 'show' });

        server_container.grab(rs_label);
        server_container.grab(rs_select);

        start_container.grab(starttime_label);
        start_container.grab(starttime_select);

        end_container.grab(endtime_label);
        end_container.grab(endtime_select);

        submit_container.grab(submit);

        form.grab(server_container);
        form.grab(start_container);
        form.grab(end_container);
        form.grab(type_select);
        form.grab(submit_container);

        rs_container.grab(form);

        new DatePicker('.start_time', { pickerClass: 'datepicker_dashboard', timePicker: true, format: 'Y-m-d-H-i',inputOutputFormat: 'Y-m-d-H-i' });
        new DatePicker('.end_time', { pickerClass: 'datepicker_dashboard', timePicker: true, format: 'Y-m-d-H-i',inputOutputFormat: 'Y-m-d-H-i' });
    },
});
