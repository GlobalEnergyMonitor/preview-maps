var config = {

    /* name of the data file; use key `csv` if data file is CSV format */
    // csv: 'GOGET_Earthgenome_file2024-04-01.csv',
    geojson: 'https://publicgemdata.nyc3.cdn.digitaloceanspaces.com/GOGET/2026-03/GOGET_map_2026-02-27.geojson',
    
    /* Define labels for sitewide colors, referenced in tracker config */
    colors: {
        'red': '#c74a48',
        'blue': '#5c62cf',
        'green': '#4c9d4f',
        'grey': '#8f8f8e',
        'black': '#000000',
    },

    /* define the column and associated values for color application */
    countryField: 'areas',
    linkField: 'id',
    urlField: 'url',

    color: {
        field: 'status-legend-global', // status-legend-global
        values: { // TODO need to add more, and adjust hyphens likely
            'operating': 'red',
            'in_development': 'blue',
            'discovered': 'blue',
            // 'shut_in': 'green', // not there
            'decommissioned': 'green', //decommissioning 
            'cancelled': 'green',
            'abandoned': 'grey',
            'ugs': 'grey', // ugs in new 
            'not-found': 'black'
        }
    },

    /* define the column and values used for the filter UI. There can be multiple filters listed. 
      Additionally a custom `label` can be defined (default is the field), 
      and `values_label` (an array matching elements in `values`)
      */
    filters: [
        {
            field: 'status-legend-global',
            values: ['operating', 'in_development', 'discovered', 'decommissioned', 'cancelled', 'abandoned', 'ugs', "not-found"],
            values_labels: ['Operating','In development','Discovered', 'Decommissioned','Cancelled','Abandoned','UGS','Not found']
        }
    ],

    /* define the field for calculating and showing capacity along with label.
       this is defined per tracker since it varies widely */
    capacityField: 'scaling-capacity',
    capacityDisplayField: 'capacity',
    // capacityLabel: 'million boe/y',
    capacityLabel: '', // (million boe/y)
    /* Labels for describing the assets */
    assetFullLabel: "Oil & Gas Extraction Areas",
    assetLabel: 'areas',

    /* the column that contains the asset name. this varies between trackers */
    nameField: 'name',
    statusDisplayField: 'status',
    /* configure the table view, selecting which columns to show, how to label them, 
        and designated which column has the link */
    
    tableHeaders: {
        values: ['name', 'operator', 'status-legend-global', 'areas', 'subnational-unit-(province,-state)', 'production---oil-(million-bbl/y)', 'production---gas-(million-m³/y)', 'production-year---oil', 'production-year---gas', 'production-start-year',],        
        labels: ['Extraction Area', 'Operator', 'Status','country/area(s)','Subnational unit (province/state)', 'Production - Oil (Million bbl/y)', 'Production - Gas (Million m³/y)', 'Production Year - Oil', 'Production Year - Gas', 'Production start year',],
        clickColumns: ['name'],
        rightAlign: ['name','discovery-year', 'fid-year', 'production-start-year', 'production---oil-(million-bbl/y)', 'production---gas-(million-m³/y)', 'production-year---oil', ],
        toLocaleString: ['production---oil-(million-bbl/y)', 'production---gas-(million-m³/y)'],
    
    
    },

    /* configure the search box; 
        each label has a value with the list of fields to search. Multiple fields might be searched */
    searchFields: { 'Extraction Area': ['name'], 
        'Companies': ['owner', 'operator', 'parent'],
        'Discovery Year': ['discovery-year'],
        'Production start year': ['production-start-year']
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
        // 'status': {'label': 'Status'},
        'loc_accuracy': {'label': 'Location Accuracy'},
        'operator': {'label': 'Operator'},
        'discovery-year': {'label': 'Discovery Year'},
        'fid-year': {'label': 'FID Year'},
        'production-start-year': {'label': 'Production Start Year'},
        'production-year---oil': {'label': 'Production Year - Oil'},
        'production-year---gas': {'label': 'Production Year - Gas'},
        'production---oil-(million-bbl/y)': {'label': 'Production - Oil (Million bbl/y)'},
        'production---gas-(million-m³/y)': {'label': 'Production - Gas (Million m³/y)'},
        'subnational-unit-(province,-state)': {'display': 'location'},
        'areas': {'display': 'location'}
    },
    countryFile: './countries.js',
    showMaxCapacity: false,
}
