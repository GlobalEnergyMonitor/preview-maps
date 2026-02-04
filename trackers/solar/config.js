var config = {
    /* name of the data file; use key `csv` if data file is CSV format */
    // csv: 'data.csv',
    geojson: 'https://publicgemdata.nyc3.cdn.digitaloceanspaces.com/solar/2026-02/solar_map_2026-02-03.geojson',

    colors: {
        'red greeninfo': '#c00',
        'light blue greeninfo': '#1e90ff',
        'blue greeninfo': '#4575b4',
        'green greeninfo': '#00b200',
        'light grey greeninfo': '#b0b0b0',
        'grey greeninfo': '#666',
        'orange greeninfo': '#fd7e14',
        'yellow greeninfo': '#ffd700'
    },

    /* define the column and associated values for color application */
    color: {
        field: 'status',
        values: {
            'operating': 'green greeninfo',
            'construction': 'yellow greeninfo',
            'pre-construction': 'orange greeninfo',
            'announced': 'red greeninfo',
            'mothballed': 'blue greeninfo',
            'shelved': 'blue greeninfo',
            'retired': 'grey greeninfo',
            'cancelled': 'grey greeninfo',

        }
    },
    interpolate: ["cubic-bezier", 0, 0, 0, 1],

    /* define the column and values used for the filter UI. There can be multiple filters listed. 
      Additionally a custom `label` can be defined (default is the field), 
      and `values_label` (an array matching elements in `values`)
      */
    filters: [
        {
            field: 'status',
            values: ['operating', 'announced', 'construction', 'pre-construction', 'mothballed', 'shelved', 'cancelled', 'retired'],
            values_labels: ['operating', 'announced', 'construction', 'pre-construction', 'mothballed', 'shelved',  'cancelled', 'retired'],
            primary: true
        },
        {
            field: 'technology-type',
            label: 'Technology Type',
            values: ['solar_thermal', 'pv', 'assumed_pv'],
            values_labels: ['Solar Thermal', 'PV', 'Assumed PV']

        },
    ],

    /* define the field for calculating and showing capacity along with label.
       this is defined per tracker since it varies widely */
    capacityField: 'capacity',
    capacityDisplayField: 'capacity',
    capacityLabel: '(MW)',

    /* Labels for describing the assets */
    assetFullLabel: "Solar photovoltaic farm phases",
    assetLabel: 'phase',

    /* the column that contains the asset name. this varies between trackers */
    nameField: 'name',
    linkField: 'pid',
    urlField: 'url',
    countryField: 'areas',

    /* configure the table view, selecting which columns to show, how to label them, 
        and designated which column has the link */
    tableHeaders: {
        values: ['name', 'phase-name', 'capacity', 'technology-type', 'status', 'start-year', 'owner', 'operator', 'location-accuracy', 'subnat', 'areas'],
        labels: ['Project', 'Phase','Capacity (MW)', 'Technology Type', 'Status', 'Start year', 'Owner', 'Operator','Location Accuracy', 'State/Province', 'Country/Area'],
        clickColumns: ['name'],
        toLocaleString: ['capacity'],
        rightAlign: ['capacity','start-year'],
        removeLastComma: ['areas'],
    },

    /* configure the search box; 
        each label has a value with the list of fields to search. Multiple fields might be searched */
        searchFields: { 'Project': ['name', 'name-in-local-language-/-script', 'other-name(s)'], 
            'Companies': ['owner', 'operator', 'owner-name-in-local-language-/-script', 'operator-name-in-local-language-/-script'],
            'Start Year': ['start-year']
        },

    /* define fields and how they are displayed. 
      `'display': 'heading'` displays the field in large type
      `'display': 'range'` will show the minimum and maximum values.
      `'display': 'join'` will join together values with a comma separator
      `'display': 'location'` will show the fields over the detail image
      `'label': '...'` prepends a label. If a range, two values for singular and plural.
    */
    detailView: {
        'name': {'display': 'heading'},
        'name-in-local-language-/-script': {'label': 'Project in Local Language / Script'},
        'owner': {'label': 'Owner'},
        'operator': {'label': 'Operator'},
        'start-year': {'label': 'Start Year'},
        'technology-type': {'label': 'Technology Type'},
        'location-accuracy': {'label': 'Location Accuracy'},
        // 'state/province': {'display': 'location'},
        // 'country/area': {'display': 'location'},
        'areas-subnat-sat-display': {'display': 'location'}


    },

    // /* radius associated with minimum/maximum value on map */
    // minRadius: 2,
    // maxRadius: 10,
    // minLineWidth: 1,
    // maxLineWidth: 10,

    // /* radius to increase min/max to under high zoom */
    // highZoomMinRadius: 4,
    // highZoomMaxRadius: 32,
    // highZoomMinLineWidth: 4,
    // highZoomMaxLineWidth: 32,

    /* radius associated with minimum/maximum value on map */
    minRadius: .8,
    maxRadius: 10,
    // /* radius to increase min/max to under high zoom */
    highZoomMinRadius: 4,
    highZoomMaxRadius: 32,
    // showAllPhases: true,

    statusField: 'status', // this strays from default, make it all the same!!
    statusDisplayField: 'status', // this strays from default, make it all the same!!
    showMinCapacity: true
}
