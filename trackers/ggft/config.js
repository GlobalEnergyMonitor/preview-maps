var config = {
    geojson:'https://publicgemdata.nyc3.cdn.digitaloceanspaces.com/ggft/2025-12/ggft_map_2025-12-03.geojson',

    // csv: '../../trialfile.csv',
    geometries: ['Point'],
    // copied starting coords from Asia map for now
    center: [60, 20],
    zoomFactor: 1.9,
    img_detail_zoom: 10,

    statusField: 'finstatus', // financing status
    statusDisplayField: 'finstatus', // need shorter names? where does ficing come from ... should not be there
    color: {

        field: 'finstatus',
        values: {  
            // 'known': 'red',
            'Known': 'red',
            'Unknown': 'blue'
        }
    },

    filters: [

        {
            field: 'finstatus',
            label: 'Financing Status',
            values: ['Known', 'Unknown'],
            values_labels: ['Known project finance','No known project finance']
        },
        {
            field: 'tab-type',
            label: 'Infrastructure Type',
            values: ['Gas Power Plants', 'LNG Terminals'],
            values_labels: ['Gas Plants', 'LNG Terminals'],
            // values_hover_text: ['hover text for fuels', '', '']
            // field_hover_text: 'Hydrogen projects are classified as either planning to blend hydrogen into methane gas or use 100% hydrogen. For the projects that plan to use hydrogen but do not specify a percentage, it is assumed they are blending. Blended projects only appear as hydrogen projects and do not also appear as methane projects, though they will use both fuel types.',
            // primary: true
        },
        {
            field: 'finbucket',//'financing', 
            label: ' Total known project finance', // info button explaining what it means
            values: ['na','low', 'mid-low', 'mid', 'mid-high', 'high'],//'high'], # mid-low', 'low', 'mid-high', 'na', 'high'
            values_labels: ['Not available', '$1-500 million', '$501-1000 million', '$1001-1500 million', '$1501-2000 million', '$2001+ million'], //'$2001-2500 million'
            // values_hover_text: ['hover tesct for fuels', '', '']
            // field_hover_text: 'GEM assesses whether hydrogen projects have met criteria (specific to each infrastructure type) demonstrating progress toward completion, since many hydrogen projects lack core details or commitments from stakeholders. For more information on these criteria, see the <a href="https://globalenergymonitor.org/projects/europe-gas-tracker/methodology/">EGT methodology page</a>'

        },

    ],

    
    /* Labels for describing the assets */
    assetFullLabel: "Units",
    assetLabel: 'units',

    capacityField: 'project-fin-scaling', // all na 'project_cap_fin_scaling', // this will be financing, and smallest value when its unknown
    linkField: 'pid',
    capacityLabel: 'million dollars', // bug with solo ones showing weird status and capacity
    showMaxCapacity: false,

    nameField: 'name', // name of projects
    countryFile: 'countries.js',
    allCountrySelect: false,
    countryField: 'areas', // country
    // multiCountry: true,
    capacityDisplayField: 'fin_by_transac', // need this since it'll sum .. need to make float
    
    tableHeaders: {
        values: ['name','unitname', 'fin', 'debtequityelse','owner', 'parent', 'importexport','opstatus', 'areas', 'startyear', 'capacitymw', 'capacitymtpa'],
        labels: ['Name','Unit','Financier', 'Financing Type','Owner', 'Parent','Terminal Facility Type', 'Operational Status','Country/Area(s)','Start year', 'Capacity (MW)', 'Capacity (MTPA)'],
        clickColumns: ['name'],
        rightAlign: ['startyear',], 
        removeLastComma: ['areas'], 
        toLocaleString: [''],


    },
    searchFields: { 'Project': ['name', 'othername', 'localname', 'name-search', 'unit-name', 'unitid', 'pid'], 
        'Project Financier': ['owner', 'parent', 'fin',  'owner-search', 'parent-search'],
   

        
    },
    detailView: {
        'name': {'display': 'heading'},
        'debt-project-financing': {'label': 'Debt Project Financing ($ million)'},
        'equity-project-financing': {'label': 'Equity Project Financing ($ million)'},
        // 'debtequityelse': {'label': 'Financing Type'},
        'startyear': {'label': 'Start Year'},
        'fin': {'label': 'Financier'},
        'opstatus': {'label': 'Operating Status'},
        'capacitymtpa': {'label': 'Capacity (mtpa)'},
        'capacitymw': {'label': 'Capacity (mw)'},
        'owner': {'label': 'Owner'},
        'parent': {'label': 'Parent'},
        'areas': {'display': 'location'}, 
    },
    // showToolTip: true,

        /* radius associated with minimum/maximum value on map */
    minRadius: 4,
    maxRadius: 10,
    // minLineWidth: 1,
    // maxLineWidth: 3,

    // /* radius to increase min/max to under high zoom */
    highZoomMinRadius: 4,
    highZoomMaxRadius: 32,
    // highZoomMinLineWidth: 4,
    // highZoomMaxLineWidth: 32,
    
    showAllPhases: true

};