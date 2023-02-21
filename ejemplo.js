{
/* //Leer las cabeceras del archivo csv
  fs.createReadStream('data.csv')
  .pipe(csv())
  .on('headers', (headers) => {
      console.log(`Header: ${headers[0]}`)
  }) */

/*   https://pharos.sh/leer-y-escribir-archivos-csv-con-node-js/

  const csv = require('csv-parser');
  const fs = require('fs');
  
  fs.createReadStream('data.csv')
    .pipe(csv())
    .on('data', (row) => {
      console.log(row);
    })
    .on('end', () => {
      console.log('CSV file successfully processed');
    }); */
}

const csv = require('csv-parser');
const fs = require('fs');

const myArray = [1,2,3]

/* function getData(file, type) {
  let data = [];
  return new Promise((resolve, reject) => {
      fs.createReadStream(file)
          .on('error', error => {
              reject(error);
          })
          .pipe(csv({headers: false, separator: ';',}))
          .on('data', (row) => {
              let item = {
                  date: row[0],
                  value: row[1]
              };
              let item2 = {
                  date: moment(row[0], "DD-MM-YYYY HH:mm").add(30, "minutes").format("DD/MM/YYYY HH:mm"),
                  value: row[2]
              };
              data.push(item);
              data.push(item2);
          })
          .on('end', () => {
              resolve(data);
          });
  });
} */

function leerFamilias(file, type){
  let filas = []
    return new Promise((resolve, reject) => {
      fs.createReadStream(file)
      .on('error', error => {
        reject(error);
      })
      .pipe(csv())
      .on('data', (row) => {
      
        filas.push(row)
          
      }).on('end', () => {
        resolve(filas);
      });
    })
  }

  async function testGetData() {
    try { 
        const data = await leerFamilias('./PRUEBAS PYTHON/HOST03_pmresult_50331648_30_202302061430_202302061500.csv', {});
        console.log("testGetData: parsed CSV data:", data);
 
    } catch (error) {
        console.error("testGetData: An error occurred: ", error.message);
    }
}

testGetData();