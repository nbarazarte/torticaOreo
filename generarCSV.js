{
  /* const createCsvWriter = require('csv-writer').createObjectCsvWriter;
  const csvWriter = createCsvWriter({
    path: 'out.csv',
    header: [
      {id: 'name', title: 'Name'},
      {id: 'surname', title: 'Surname'},
      {id: 'age', title: 'Age'},
      {id: 'gender', title: 'Gender'},
    ]
  });
  
  const data = [
    {
      name: 'John',
      surname: 'Snow',
      age: 26,
      gender: 'M'
    }, {
      name: 'Clair',
      surname: 'White',
      age: 33,
      gender: 'F',
    }, {
      name: 'Fancy',
      surname: 'Brown',
      age: 78,
      gender: 'F'
    }
  ];
  
  csvWriter
    .writeRecords(data)
    .then(()=> console.log('The CSV file was written successfully'));  */ 
}
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const csvWriter = createCsvWriter({
  path: 'out.csv',
  header: [
    {id: '50331745', title: '50331745'},
    {id: '50331746', title: '50331746'},
  ]
});

/* const data = [
  {
    50331745: 'None', 
    50331746: 'bit'
}, 
  { 
    50331745: 884951, 
    50331746: 3490623989
  }
]; */

const data = [
  { '50331745': 'None', '50331746': 'bit' },
  { '50331745': '256823', '50331746': '929454878' },
  { '50331745': '232699', '50331746': '509266149' },
  { '50331745': '1327388', '50331746': '4900173566' },
  { '50331745': '230195', '50331746': '782669458' },
  { '50331745': '141553', '50331746': '470874852' },
  { '50331745': '1216966', '50331746': '4985445177' },
  { '50331745': '369799', '50331746': '1572421203' },
  { '50331745': '486401', '50331746': '1496143253' },
  { '50331745': '1643762', '50331746': '4308158937' }
]



csvWriter
  .writeRecords(data)
  .then(()=> console.log('The CSV file was written successfully')); 