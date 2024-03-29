/***********************************************************************/
//  Desarrollado por: Coordinación de Estadísticas Operativas (CEO)    *
//  Fecha: 19/02/2023                                                  *
/***********************************************************************/
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const { log } = require("console");
const csv = require("csv-parser");
const fs = require("fs");
/*********************************************** Lee las cabeceras (contadores) y filas del archivo de familias ***************************************************************/

function leerFilas(file, type) {
  let filas = [];
  return new Promise((resolve, reject) => {
    fs.createReadStream(file)
      .on("error", (error) => {
        reject(error);
      })
      .pipe(csv())
      .on("data", (row) => {
        filas.push(row);
      })
      .on("end", () => {
        resolve(filas);
      });
  });
}

function leerDirectorio_unificado() {
  const files = fs.readdirSync("./UNIFICADOS/");
  let familia;
  let familias = [];
  const columnasComunes = [
    "Result Time",
    "Granularity Period",
    "Object Name",
    "Reliability",
  ];
  const ruta = "./UNIFICADOS/";

  for (const x of files) {
    nombreFamilia = x.slice(0, 8);

    if (!familias.includes(nombreFamilia)) {
      familias.push(nombreFamilia);
    }
  }

  for (const x of familias) {
    if (x === "50331648") {
      familia = ruta + x + ".csv";
      contadores = ["50331745", "50331746"];
      kpi = ["HSDPA_THROUGHPUT"];

      procesarFamilia(
        familia,
        columnasComunes.concat(contadores),
        columnasComunes.concat(kpi)
      );


    }

    if (x === "67109365") {
      familia = ruta + x + ".csv";
      contadores = [
        "67179329",
        "67179330",
        "67179331",
        "67179332",
        "67179333",
        "67179334",
        "67179335",
        "67179336",
        "67179337",
        "67179338",
        "67179343",
        "67179344",
        "67179345",
        "67179346",
        "67179347",///
        "67179348",
        "67179457",
        "67179458",
        "67179459",
        "67179460",
        "67179461",
        "67179462",
        "67179463",
        "67179464",
        "67179465",
        "67179466",
        "67179471",
        "67179472",
        "67179473",
        "67179474",
        "67179476",
        "67190586",
      ];
      kpi = [
        "U_RRC_SETUP_SUCCESS_RATIO_DEN_NONE",
        "U_RRC_SETUP_SUCCESS_RATIO_NUM_NONE",
      ];


      procesarFamilia(
        familia,
        columnasComunes.concat(contadores),
        columnasComunes.concat(kpi)
      );


    }
    /*
    if(x === '67109372'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['67179921','67179922','67179923','67179924','67179925','67179926','67179927','67179928']      
    }

    if(x === '67109368'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['67179825','67179826','67179827','67179828']      
    }
    
    if(x === '67109471'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['67192486','67203850','67203932','73403761','73410507','73441239']
    }
    
    if(x === '67109369'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['67179864','67190406','67190407','67192610','67192611','67193611','67193612']
    }    
    
    if(x === '67109367'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['67190404','67190405','67192608','67192609','67193609','67193610']
    }      
    
    if(x === '67109508'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['67192690','67192689']
    }      
    
    if(x === '67109391'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['67192584','73421766']
    }     
    
    if(x === '67109373'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['67179967','67190408','67190409','67192612','67192613','67193613','67193614','73424888','73424889']
    }      
    
    if(x === '82834952'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['73410510','73410511']
    }     
    
    if(x === '82863958'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['73424897','73424899']      
    }     
    
    if(x === '67109376'){
      host03 = ruta+x+'_HOST03.csv'
      host12 = ruta+x+'_HOST12.csv'      
      contadores = ['67179778','67179779','67179781','67179782','73421882','73421883','73421886','73422166']
    } */


  }
}

function recorrerFamiliaContadores(rows, contadores) {
  let newHeader = [],
    arreglo = [];
  for (let y = 0; y < rows.length; y++) {
    let miObjeto = {};
    for (let x = 0; x < contadores.length; x++) {
      miObjeto[contadores[x]] = rows[y][contadores[x]];
      newHeader[x] = { id: contadores[x], title: contadores[x] };
    }
    arreglo.push(miObjeto);
  }

  return arreglo;
}

async function procesarFamilia(familia, contadores, kpi) {
  try {
    let arreglo = [],
      arregloFinal = [];
    const fechaHoraEjecucion = new Date().toLocaleString("es-ES", {
      timeZone: "America/Caracas",
    });
    const rows = await leerFilas(familia, {});
    //let newHeader = [],
    newHeaderFinal = [];

    arreglo = recorrerFamiliaContadores(rows, contadores);

    let fam = familia.slice(13, 21);
  
    switch (fam) {
      case "50331648":
     
        for (let y = 1; y < arreglo.length; y++) {
          let miObjeto2 = {};
          let res;
    
          if (Number(arreglo[y]["50331745"]) == 0) {
            res = 0;
          } else {
            res = Number(arreglo[y]["50331746"]) / Number(arreglo[y]["50331745"]);
          }
    
          for (let x = 0; x < kpi.length; x++) {
            if (kpi[x] === "HSDPA_THROUGHPUT") {
              miObjeto2[kpi[x]] = res;
            } else {
              miObjeto2[kpi[x]] = rows[y][kpi[x]];
            }
    
            newHeaderFinal[x] = { id: kpi[x], title: kpi[x] };
          }
          arregloFinal.push(miObjeto2);
        }

      break;

      case '67109365':

        for (let y = 1; y < arreglo.length; y++) {
          let miObjeto2 = {};
          let res_den = 0, res_num = 0;

          res_den = Number(arreglo[y]['67179329'])+Number(arreglo[y]['67179330'])+Number(arreglo[y]['67179331'])+Number(arreglo[y]['67179332'])+Number(arreglo[y]['67179333'])+Number(arreglo[y]['67179334'])+Number(arreglo[y]['67179335'])+Number(arreglo[y]['67179336'])+Number(arreglo[y]['67179337'])+Number(arreglo[y]['67179338'])+Number(arreglo[y]['67179348'])+Number(arreglo[y]['67179343'])+Number(arreglo[y]['67179344'])+Number(arreglo[y]['67179345'])+Number(arreglo[y]['67179346'])+Number(arreglo[y]['67179347']);
          res_num = Number(arreglo[y]['67179457'])+Number(arreglo[y]['67179458'])+Number(arreglo[y]['67179459'])+Number(arreglo[y]['67179460'])+Number(arreglo[y]['67179461'])+Number(arreglo[y]['67179462'])+Number(arreglo[y]['67179463'])+Number(arreglo[y]['67179464'])+Number(arreglo[y]['67179465'])+Number(arreglo[y]['67179466'])+Number(arreglo[y]['67179476'])+Number(arreglo[y]['67179471'])+Number(arreglo[y]['67179472'])+Number(arreglo[y]['67179473'])+Number(arreglo[y]['67179474']);
    
          for (let x = 0; x < kpi.length; x++) {
  
            if (kpi[x] === "U_RRC_SETUP_SUCCESS_RATIO_NUM_NONE") {
              miObjeto2[kpi[x]] = res_num;
            } else if (kpi[x] === "U_RRC_SETUP_SUCCESS_RATIO_DEN_NONE") {
              miObjeto2[kpi[x]] = res_den;
            }else{
              miObjeto2[kpi[x]] = rows[y][kpi[x]];
            }
            
            newHeaderFinal[x] = { id: kpi[x], title: kpi[x] };
          }
          arregloFinal.push(miObjeto2);
        }

      break;

    }

    //console.log(arregloFinal);
    /*********************************************** Escribe la salida en un nuevo archivo csv con sus contadores especificos ******************************************************/
    const csvWriter = createCsvWriter({
      path: "./FAMILIAKPI/" + familia.slice(13, 25),
      header: newHeaderFinal,
    });

    csvWriter
      .writeRecords(arregloFinal)
      .then(() =>
        console.log(
          `⁙ Creando el archivo con los kpis ${familia.slice(13, 21)}.csv de la familia ${familia.slice(13, 21)} con éxito. - Fin: ${fechaHoraEjecucion} ⁙\n`
        )
      );
    /********************************************************************************************************************************************************************************/
  } catch (error) {
    console.error(`🐞 Error:`, error.message);
  }
}

leerDirectorio_unificado();
