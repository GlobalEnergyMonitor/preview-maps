var config = {
    // geojson: 'https://publicgemdata.nyc3.cdn.digitaloceanspaces.com/ggit/2025-11/europe_map_2025-11-25.geojson',
    csv: '../../trialfile.csv',
    // geometries: ['Point'],
    // copied starting coords from Asia map for now
    center: [60, 20],
    zoomFactor: 1.9,
    img_detail_zoom: 10,

    statusField: 'status-legend', // financing status
    statusDisplayField: 'status',
    color: {

        field: 'status-legend',
        values: {  
            'known': 'red',
            'unknown': 'blue',
        }
    },

    filters: [

        {
            field: 'status-legend',
            label: 'Financing Status',
            values: ['known','unknown'],
            values_labels: ['Known project financing','No known financing',]
        },
        {
            field: 'infra-type',
            label: 'Infrastructure Type',
            values: ['gas', 'lng'],
            values_labels: ['Gas Plants', 'LNG Terminals'],
            // values_hover_text: ['hover text for fuels', '', '']
            // field_hover_text: 'Hydrogen projects are classified as either planning to blend hydrogen into methane gas or use 100% hydrogen. For the projects that plan to use hydrogen but do not specify a percentage, it is assumed they are blending. Blended projects only appear as hydrogen projects and do not also appear as methane projects, though they will use both fuel types.',
            // primary: true
        },
        {
            field: 'financing', 
            label: ' Total known project finance', // info button explaining what it means
            values: ['na','low', 'mid-low', 'mid', 'mid-high', 'high'],
            values_labels: ['Not available', '$1-500 million', '$501-1000 million', '$1001-1500 million', '$1500-2000 million', '$2001-2500 million'],
            // values_hover_text: ['hover tesct for fuels', '', '']
            // field_hover_text: 'GEM assesses whether hydrogen projects have met criteria (specific to each infrastructure type) demonstrating progress toward completion, since many hydrogen projects lack core details or commitments from stakeholders. For more information on these criteria, see the <a href="https://globalenergymonitor.org/projects/europe-gas-tracker/methodology/">EGT methodology page</a>'

        },

    ],
    capacityField: 'scaling-value', // this will be financing, and smallest value when its unknown
    linkField: 'pid',
    capacityLabel: '',
    showMaxCapacity: false,

    nameField: 'name', // name of projects
    countryFile: 'countries.js',
    allCountrySelect: false,
    countryField: 'areas', // country
    multiCountry: true,
    capacityDisplayField: 'actual-financing-value',
    
    tableHeaders: {
        values: ['name','unit-name', 'owner', 'parent', 'capacity-table', 'units-of-m','status', 'areas', 'start-year', 'prod-gas', 'prod-year-gas','tracker-display'],
        labels: ['Name','Unit','Owner', 'Parent','Capacity', 'units','Status','Country/Area(s)','Start year', 'Production (Million m³/y)', 'Production year (gas)', 'Type'],
        clickColumns: ['name'],
        rightAlign: ['capacity-table','prod-gas','start-year','prod-year-gas'], 
        removeLastComma: ['areas'], 
        toLocaleString: ['capacity-table'],


    },
    searchFields: { 'Project': ['name', 'other-name', 'local-name'], 
        // 'Companies': ['owner', 'parent'],
        // 'Start Year': ['start-year'],
        // 'Infrastructure Type': ['tracker-display'],
        // 'Status': ['status'],
        // 'Province/State': ['subnat']
    },
    detailView: {
        'name': {'display': 'heading'},
        'prod-gas': {'label': 'Production (Million m³/y)'},
        'prod-year-gas': {'label': 'Production Year - Gas'},
        'start-year': {'label': 'Start Year'},
        'owner': {'label': 'Owner'},
        'parent': {'label': 'Parent'},
        // 'river': {'label': 'River'},
        // 'tracker-display': {'label': 'Type'},
        'areas': {'label': 'Country/Area(s)'},
        'areas-subnat-sat-display': {'display': 'location'}, 
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