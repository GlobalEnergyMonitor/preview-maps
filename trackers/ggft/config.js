var config = {
    geojson:'https://publicgemdata.nyc3.cdn.digitaloceanspaces.com/ggft/2025-12/ggft_map_2025-12-03.geojson',

    // csv: '../../trialfile.csv',
    geometries: ['Point'],
    // copied starting coords from Asia map for now
    center: [60, 20],
    zoomFactor: 1.9,
    img_detail_zoom: 10,

    statusField: 'finstatus', // financing status
    statusDisplayField: 'finstatus',
    color: {

        field: 'finstatus',
        values: {  
            // 'known': 'red',
            '<NA>': 'red',
            'unknown': 'blue',
            'Not available': 'blue'
        }
    },

    filters: [

        {
            field: 'finstatus',
            label: 'Financing Status',
            values: ['<NA>','unknown', 'Not available'],
            values_labels: ['Known project financing','No known financing', 'Not Found']
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
            field: 'project-cap-fin-scaling',//'financing', 
            label: ' Total known project finance', // info button explaining what it means
            values: ['Not available','low', 'mid-low', 'mid', 'mid-high', 'Not availableNot available'],//'high'],
            values_labels: ['Not available', '$1-500 million', '$501-1000 million', '$1001-1500 million', '$1500-2000 million', '$2001-2500 million'],
            // values_hover_text: ['hover tesct for fuels', '', '']
            // field_hover_text: 'GEM assesses whether hydrogen projects have met criteria (specific to each infrastructure type) demonstrating progress toward completion, since many hydrogen projects lack core details or commitments from stakeholders. For more information on these criteria, see the <a href="https://globalenergymonitor.org/projects/europe-gas-tracker/methodology/">EGT methodology page</a>'

        },

    ],

    
    /* Labels for describing the assets */
    assetFullLabel: "Projects",
    assetLabel: 'projects',

    capacityField: 'scaling-capacity', // all na 'project_cap_fin_scaling', // this will be financing, and smallest value when its unknown
    linkField: 'pid',
    capacityLabel: '$',
    showMaxCapacity: false,

    nameField: 'name', // name of projects
    countryFile: 'countries.js',
    allCountrySelect: false,
    countryField: 'areas', // country
    multiCountry: true,
    capacityDisplayField: 'project_cap_fin_scaling',
    
    tableHeaders: {
        values: ['name','unit-name', 'fin', 'debtequityelse','owner', 'parent', 'importexport','status', 'areas', 'startyear', 'capacitymw', 'capacitymtpa','debtequityelse'],
        labels: ['Name','Unit','Financier', 'Financing Type','Owner', 'Parent','units','Terminal Facility Type', 'Status','Country/Area(s)','Start year', 'Capacity (MW)', 'Capacity (MTPA)', 'Financing Type'],
        clickColumns: ['name'],
        rightAlign: ['cleaned_cap','startyear',], 
        removeLastComma: ['areas'], 
        toLocaleString: ['cleaned_cap'],


    },
    searchFields: { 'Project': ['name', 'othername', 'localname', 'name-search', 'owner-search', 'parent-search'], 
        'Financiers/Ownership': ['owner', 'parent', 'fin'],
        'Start Year': ['startyear'],
        'Operational Status': ['opstatus'],
        // 'Status': ['status'],
        // 'Province/State': ['subnat']
    },
    detailView: {
        'name': {'display': 'heading'},
        'prod-gas': {'label': 'Production (Million m³/y)'},
        'prod-year-gas': {'label': 'Production Year - Gas'},
        'startyear': {'label': 'Start Year'},
        'fin': {'label': 'Financier'},
        'owner': {'label': 'Owner'},
        'parent': {'label': 'Parent'},
        'areas': {'display': 'location'}, 
    },
    // showToolTip: true,

        /* radius associated with minimum/maximum value on map */
    // minRadius: 2,
    // maxRadius: 10,
    // minLineWidth: 1,
    // maxLineWidth: 3,

    // /* radius to increase min/max to under high zoom */
    // highZoomMinRadius: 4,
    // highZoomMaxRadius: 32,
    // highZoomMinLineWidth: 4,
    // highZoomMaxLineWidth: 32,
    
    // showAllPhases: true

};