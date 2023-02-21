/***********************************************************************/
//  Desarrollado por: Coordinación de Estadísticas Operativas (CEO)    *
//  Fecha: 19/02/2023                                                  *
/***********************************************************************/
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const csv = require('csv-parser');
const fs = require('fs');

/*********************************************** Lee las cabeceras (contadores) y filas del archivo de familias ***************************************************************/
function leerCabeceras(file, type){
    return new Promise((resolve, reject) => {
        fs.createReadStream(file)
        .on('error', error => {
            reject(error);
        })
        .pipe(csv())
        .on('headers', (headers) => {
            //console.log(headers)
            resolve(headers);
        })
    })
}

function leerFilas(file, type){
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

async function procesar(familia, contadores) {
    try { 
          const file = familia
          const newColumns = []
          const arregloFinal = []
          const columns = await leerCabeceras(file, {});//console.log("Columnas:", columns[4]); //console.log(columns.length); 
          const rows  = await leerFilas(file, {});//console.log("Filas:", rows[0][columns[4]]); //console.log(rows.length);
          const nombreFamilia = familia.slice(33, -33)
          const fecha = `${familia.slice(58, -8).split('')[6]}${familia.slice(58, -8).split('')[7]}/${familia.slice(58, -8).split('')[4]}${familia.slice(58, -8).split('')[5]}/${familia.slice(58, -8).split('')[0]}${familia.slice(58, -8).split('')[1]}${familia.slice(58, -8).split('')[2]}${familia.slice(58, -8).split('')[3]}`
          const hora = `${familia.slice(66,-4).split('')[0]}${familia.slice(66,-4).split('')[1]}:${familia.slice(66,-4).split('')[2]}${familia.slice(66,-4).split('')[3]}`
          const fechaHoraEjecucion = new Date().toLocaleString("es-ES", { timeZone: "America/Caracas" });
          console.log(`⏳ Procesando familia ${nombreFamilia} - 📆Fecha: ${fecha} ⌚Hora: ${hora} - Inicio: ${fechaHoraEjecucion} \n`)
        
          for (const x of contadores) {
            newColumns.push(x)
          }

          let noExiste = []  
          try { 

            for (const x of contadores) {  
              for (const y of columns) {              
                if(!columns.includes(x)){
                  noExiste.push(x)
                  flag = true
                  throw "";
                }
              }
            }
          }
          catch(err) {
            console.error(`🐞 Falta(n) contador(es): ${noExiste} en la familia ${nombreFamilia} - Hora: ${fechaHoraEjecucion} 👺 \n`, err);
            return   
          }

          for (let y = 0; y < rows.length; y++) {
            let miObjeto = {}
            for (let x = 0; x < newColumns.length; x++) {
              miObjeto[newColumns[x]] = rows[y][newColumns[x]]
           }
          //console.log(miObjeto);
            arregloFinal.push(miObjeto)
          }
         //console.log(arregloFinal);
/*********************************************** Escribe la salida en un nuevo archivo csv con sus contadores especificos ******************************************************/
          let newHeader = []         
          for (let x = 0; x < contadores.length; x++) {
            //console.log(contadores[x])
            newHeader[x] = {'id': contadores[x], 'title':contadores[x]}
          }
          //console.log(newHeader);

          const csvWriter = createCsvWriter({
          path: nombreFamilia+'.csv',
          header: newHeader
          /*header: [
            {id: 'Result Time', title: 'Result Time'},
            {id: 'Granularity Period', title: 'Granularity Period'},
            {id: 'Object Name', title: 'Object Name'},
            {id: 'Reliability', title: 'Reliability'},
            {id: '50331745', title: '50331745'},
            {id: '50331746', title: '50331746'},            
          ] */
        });

         csvWriter
         .writeRecords(arregloFinal)
         .then(()=> console.log(`✔ Se creó el archivo de contadores ${nombreFamilia}.csv de la familia ${nombreFamilia} con éxito. - Fin: ${fechaHoraEjecucion} 👽 \n`)); 
/********************************************************************************************************************************************************************************/
        } catch (error) {
            console.error(`🐞 Ocurrió el siguiente error: 👺 - Hora: ${new Date}`, error.message);
        }
}

//Pendiente: hacer funcion que lea el directorio de los archivos a procesar!!!!!!!!!!!!!!!!!!

//Columnas comunes de todas las familias:
const columnasComunes = ['Result Time','Granularity Period','Object Name','Reliability']

const contadores_50331648 = ['50331745', '50331746']
procesar('./PRUEBAS PYTHON/HOST12_pmresult_50331648_30_202302101300_202302101330.csv', columnasComunes.concat(contadores_50331648));
 
const contadores_67109365 = ['67179329','67179330','67179331','67179332','67179333','67179334','67179335','67179336','67179337','67179338','67179343','67179344','67179345','67179346','67179347','67179348','67179457','67179458','67179459','67179460','67179461','67179462','67179463','67179464','67179465','67179466','67179471','67179472','67179473','67179474','67179476','67190586']
procesar('./PRUEBAS PYTHON/HOST03_pmresult_67109365_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109365));

const contadores_67109372 = ['67179921','67179922','67179923','67179924','67179925','67179926','67179927','67179928']
procesar('./PRUEBAS PYTHON/HOST03_pmresult_67109372_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109372));

const contadores_67109368 = ['67179825','67179826','67179827','67179828']
procesar('./PRUEBAS PYTHON/HOST12_pmresult_67109368_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109368));

const contadores_67109471 = ['67192486','67203850','67203932','73403761','73410507','73441239']
procesar('./PRUEBAS PYTHON/HOST03_pmresult_67109471_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109471));

const contadores_67109369 = ['67179864','67190406','67190407','67192610','67192611','67193611','67193612']
procesar('./PRUEBAS PYTHON/HOST03_pmresult_67109369_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109369));

const contadores_67109367 = ['67190404','67190405','67192608','67192609','67193609','67193610']
procesar('./PRUEBAS PYTHON/HOST03_pmresult_67109367_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109367));

const contadores_67109508 = ['67192690','67192689']
procesar('./PRUEBAS PYTHON/HOST03_pmresult_67109508_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109508));

const contadores_67109391 = ['67192584','73421766']
procesar('./PRUEBAS PYTHON/HOST03_pmresult_67109391_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109391));

const contadores_67109373 = ['67179967','67190408','67190409','67192612','67192613','67193613','67193614','73424888','73424889']
procesar('./PRUEBAS PYTHON/HOST12_pmresult_67109373_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109373));

const contadores_82834952 = ['73410510','73410511']
procesar('./PRUEBAS PYTHON/HOST12_pmresult_82834952_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_82834952));

const contadores_82863958 = ['73424897','73424899']
procesar('./PRUEBAS PYTHON/HOST12_pmresult_82863958_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_82863958));

const contadores_67109376 = ['67179778','67179779','67179781','67179782','73421882','73421883','73421886','73422166']
procesar('./PRUEBAS PYTHON/HOST03_pmresult_67109376_30_202302101300_202302101330.csv',columnasComunes.concat(contadores_67109376));

//Por Hacer:
for (let x = 0; x < 3; x++) {
  //leerDirectorio()
}

function leerDirectorio(){
  console.log('Leyendo directorio');
}




// /***********************************************************************/
// //  Desarrollado por: Coordinación de Estadísticas Operativas (CEO)    *
// //  Fecha: 19/02/2023                                                  *
// /***********************************************************************/
// const createCsvWriter = require('csv-writer').createObjectCsvWriter;
// const csv = require('csv-parser');
// const fs = require('fs');
// /*********************************************** Lee las cabeceras (contadores) y filas del archivo de familias ***************************************************************/
// function leerCabeceras(file, type,contadores,nombreFamilia,host){
//   return new Promise((resolve, reject) => {
//       fs.createReadStream(file)
//       .on('error', error => {
//           reject(error);
//       })
//       .pipe(csv())
//       .on('headers', (headers) => {
//           //console.log(headers)
//           for (const x of contadores) {  
//             for (const y of headers) { 
//               if(!headers.includes(x)){
//                 reject(new Error(`Falta(n) ${x} en la familia ${nombreFamilia} del host ${host} - Hora: ${new Date} \n`));      
//               }
//             }
//           }
//         resolve(headers);
//       })
//   })
// }

// function leerFilas(file, type){
//     let filas = []
//       return new Promise((resolve, reject) => {
//         fs.createReadStream(file)
//         .on('error', error => {
//           reject(error);
//         })
//         .pipe(csv())
//         .on('data', (row) => {
//           filas.push(row)
//         }).on('end', () => {
//           resolve(filas);
//         });
//       })
// }

// function leerDirectorio(){

//   const files = fs.readdirSync('./PRUEBAS PYTHON') 
//   let nombreFamilia
//   let host

//   for (const x of files) {
  
//     nombreFamilia = x.slice(16, -33)
//     host = x.slice(0, 6)

//     if(nombreFamilia === '50331648'){
//       const contadores = ['50331745', '50331746']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }
 
//     if(nombreFamilia === '67109365'){
//       const contadores = ['67179329','67179330','67179331','67179332','67179333','67179334','67179335','67179336','67179337','67179338','67179343','67179344','67179345','67179346','67179347','67179348','67179457','67179458','67179459','67179460','67179461','67179462','67179463','67179464','67179465','67179466','67179471','67179472','67179473','67179474','67179476','67190586']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }

//     if(nombreFamilia === '67109372'){
//       const contadores = ['67179921','67179922','67179923','67179924','67179925','67179926','67179927','67179928']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }
         
//     if(nombreFamilia === '67109368'){
//       const contadores = ['67179825','67179826','67179827','67179828']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }
    
//     if(nombreFamilia === '67109471'){
//       const contadores = ['67192486','67203850','67203932','73403761','73410507','73441239']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }
    
//     if(nombreFamilia === '67109369'){
//       const contadores = ['67179864','67190406','67190407','67192610','67192611','67193611','67193612']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }    
    
//     if(nombreFamilia === '67109367'){
//       const contadores = ['67190404','67190405','67192608','67192609','67193609','67193610']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }      
    
//     if(nombreFamilia === '67109508'){
//       const contadores = ['67192690','67192689']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }      
    
//     if(nombreFamilia === '67109391'){
//       const contadores = ['67192584','73421766']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }     
    
//     if(nombreFamilia === '67109373'){
//       const contadores = ['67179967','67190408','67190409','67192612','67192613','67193613','67193614','73424888','73424889']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }      
    
//     if(nombreFamilia === '82834952'){
//       const contadores = ['73410510','73410511']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }     
    
//     if(nombreFamilia === '82863958'){
//       const contadores = ['73424897','73424899']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }     
    
//     if(nombreFamilia === '67109376'){
//       const contadores = ['67179778','67179779','67179781','67179782','73421882','73421883','73421886','73422166']
//       const ruta = './PRUEBAS PYTHON/'+x
//       comprobarCrudosCabeceras(ruta,contadores, host)
//     }  
//   }
//   console.log(`Finalizó la comprobación de los contadores\n`);
// }

// async function comprobarCrudosCabeceras(familia, contadores,host) {
//   try { 
//         const file = familia
//         const nombreFamilia = familia.slice(33, -33)
//         const fecha = `${familia.slice(58, -8).split('')[6]}${familia.slice(58, -8).split('')[7]}/${familia.slice(58, -8).split('')[4]}${familia.slice(58, -8).split('')[5]}/${familia.slice(58, -8).split('')[0]}${familia.slice(58, -8).split('')[1]}${familia.slice(58, -8).split('')[2]}${familia.slice(58, -8).split('')[3]}`
//         const hora = `${familia.slice(66,-4).split('')[0]}${familia.slice(66,-4).split('')[1]}:${familia.slice(66,-4).split('')[2]}${familia.slice(66,-4).split('')[3]}`
//         const fechaHoraEjecucion = new Date().toLocaleString("es-ES", { timeZone: "America/Caracas" });
        
//         await leerCabeceras(file, {}, contadores,nombreFamilia,host);//console.log("Columnas:", columns[4]); //console.log(columns.length); 
//         //console.log(`⏳ Leyendo archivo crudo de la familia ${nombreFamilia} - 📆Fecha: ${fecha} ⌚Hora: ${hora} - Inicio: ${fechaHoraEjecucion} \n`)
      
//       } catch (error) {
//           console.error(`🐞 Error de contadores:`, error.message);
//       }
// }

// function leerDirectorio_procesar(){

//   const files = fs.readdirSync('./PRUEBAS PYTHON') 
//   let nombreFamilia
//   let host

//   for (const x of files) {
  
//     nombreFamilia = x.slice(16, -33)
//     host = x.slice(0, 6)

//     if(nombreFamilia === '50331648'){
//       const contadores = ['50331745', '50331746']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }
 
//     if(nombreFamilia === '67109365'){
//       const contadores = ['67179329','67179330','67179331','67179332','67179333','67179334','67179335','67179336','67179337','67179338','67179343','67179344','67179345','67179346','67179347','67179348','67179457','67179458','67179459','67179460','67179461','67179462','67179463','67179464','67179465','67179466','67179471','67179472','67179473','67179474','67179476','67190586']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }

//     if(nombreFamilia === '67109372'){
//       const contadores = ['67179921','67179922','67179923','67179924','67179925','67179926','67179927','67179928']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }
         
//     if(nombreFamilia === '67109368'){
//       const contadores = ['67179825','67179826','67179827','67179828']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }
    
//     if(nombreFamilia === '67109471'){
//       const contadores = ['67192486','67203850','67203932','73403761','73410507','73441239']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }
    
//     if(nombreFamilia === '67109369'){
//       const contadores = ['67179864','67190406','67190407','67192610','67192611','67193611','67193612']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }    
    
//     if(nombreFamilia === '67109367'){
//       const contadores = ['67190404','67190405','67192608','67192609','67193609','67193610']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }      
    
//     if(nombreFamilia === '67109508'){
//       const contadores = ['67192690','67192689']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }      
    
//     if(nombreFamilia === '67109391'){
//       const contadores = ['67192584','73421766']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }     
    
//     if(nombreFamilia === '67109373'){
//       const contadores = ['67179967','67190408','67190409','67192612','67192613','67193613','67193614','73424888','73424889']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }      
    
//     if(nombreFamilia === '82834952'){
//       const contadores = ['73410510','73410511']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }     
    
//     if(nombreFamilia === '82863958'){
//       const contadores = ['73424897','73424899']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }     
    
//     if(nombreFamilia === '67109376'){
//       const contadores = ['67179778','67179779','67179781','67179782','73421882','73421883','73421886','73422166']
//       const ruta = './PRUEBAS PYTHON/'+x
//       procesar(ruta,contadores, host)
//     }
//   }
// }

// async function procesar(familia, contadores,host) {
//   try { 
//         const file = familia
//         const newColumns = []
//         const arregloFinal = []

//         const nombreFamilia = familia.slice(33, -33)
//         const fecha = `${familia.slice(58, -8).split('')[6]}${familia.slice(58, -8).split('')[7]}/${familia.slice(58, -8).split('')[4]}${familia.slice(58, -8).split('')[5]}/${familia.slice(58, -8).split('')[0]}${familia.slice(58, -8).split('')[1]}${familia.slice(58, -8).split('')[2]}${familia.slice(58, -8).split('')[3]}`
//         const hora = `${familia.slice(66,-4).split('')[0]}${familia.slice(66,-4).split('')[1]}:${familia.slice(66,-4).split('')[2]}${familia.slice(66,-4).split('')[3]}`
//         const fechaHoraEjecucion = new Date().toLocaleString("es-ES", { timeZone: "America/Caracas" });
        
//         await leerCabeceras(file, {},contadores,nombreFamilia,host);//console.log("Columnas:", columns[4]); //console.log(columns.length); 
//         const rows  = await leerFilas(file, {});//console.log("Filas:", rows[0][columns[4]]); //console.log(rows.length);
//         //console.log(`⏳ Procesando familia ${nombreFamilia} - 📆Fecha: ${fecha} ⌚Hora: ${hora} - Inicio: ${fechaHoraEjecucion} \n`)
      
//         for (const x of contadores) {
//           newColumns.push(x)
//         }

//         for (let y = 0; y < rows.length; y++) {
//           let miObjeto = {}
//           for (let x = 0; x < newColumns.length; x++) {
//             miObjeto[newColumns[x]] = rows[y][newColumns[x]]
//          }
//         //console.log(miObjeto);
//           arregloFinal.push(miObjeto)
//         }
//        //console.log(arregloFinal);
// /*********************************************** Escribe la salida en un nuevo archivo csv con sus contadores especificos ******************************************************/
//         let newHeader = []         
//         for (let x = 0; x < contadores.length; x++) {
//           //console.log(contadores[x])
//           newHeader[x] = {'id': contadores[x], 'title':contadores[x]}
//         }
//         //console.log(newHeader);

//         const csvWriter = createCsvWriter({
//         path: nombreFamilia+'.csv',
//         header: newHeader
//         /*header: [
//           {id: 'Result Time', title: 'Result Time'},
//           {id: 'Granularity Period', title: 'Granularity Period'},
//           {id: 'Object Name', title: 'Object Name'},
//           {id: 'Reliability', title: 'Reliability'},
//           {id: '50331745', title: '50331745'},
//           {id: '50331746', title: '50331746'},            
//         ] */
//       });

//        csvWriter
//        .writeRecords(arregloFinal)
//        .then(()=> console.log(`✔ ★ Se creó el archivo de contadores ${nombreFamilia}.csv de la familia ${nombreFamilia} con éxito. - Fin: ${fechaHoraEjecucion} 👽 \n`)); 
// /********************************************************************************************************************************************************************************/
//       } catch (error) {
//           console.error(`🐞 Error:`, error.message);
//       }
// }

// //leerDirectorio()
// leerDirectorio_procesar()