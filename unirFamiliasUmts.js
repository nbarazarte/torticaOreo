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

function leerDirectorio_procesar() {
  const files = fs.readdirSync("./CONVERTIDOS/");
  let familias = [];
  let contadores;
  let host03, host12;
  const columnasComunes = [
    "Result Time",
    "Granularity Period",
    "Object Name",
    "Reliability",
  ];
  const ruta = "./CONVERTIDOS/";

  for (const x of files) {
    nombreFamilia = x.slice(0, -11);
    if (!familias.includes(nombreFamilia)) {
      familias.push(nombreFamilia);
    }
  }

  for (const x of familias) {
    if (x === "50331648") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = ["50331745", "50331746"];
    }

    if (x === "67109365") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
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
        "67179347",
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
    }

    if (x === "67109372") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = [
        "67179921",
        "67179922",
        "67179923",
        "67179924",
        "67179925",
        "67179926",
        "67179927",
        "67179928",
      ];
    }

    if (x === "67109368") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = ["67179825", "67179826", "67179827", "67179828"];
    }

    if (x === "67109471") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = [
        "67192486",
        "67203850",
        "67203932",
        "73403761",
        "73410507",
        "73441239",
      ];
    }

    if (x === "67109369") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = [
        "67179864",
        "67190406",
        "67190407",
        "67192610",
        "67192611",
        "67193611",
        "67193612",
      ];
    }

    if (x === "67109367") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = [
        "67190404",
        "67190405",
        "67192608",
        "67192609",
        "67193609",
        "67193610",
      ];
    }

    if (x === "67109508") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = ["67192690", "67192689"];
    }

    if (x === "67109391") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = ["67192584", "73421766"];
    }

    if (x === "67109373") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = [
        "67179967",
        "67190408",
        "67190409",
        "67192612",
        "67192613",
        "67193613",
        "67193614",
        "73424888",
        "73424889",
      ];
    }

    if (x === "82834952") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = ["73410510", "73410511"];
    }

    if (x === "82863958") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = ["73424897", "73424899"];
    }

    if (x === "67109376") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = [
        "67179778",
        "67179779",
        "67179781",
        "67179782",
        "73421882",
        "73421883",
        "73421886",
        "73422166",
      ];
    }

    if (x === "67109390") {
      host03 = ruta + x + "_HOST03.csv";
      host12 = ruta + x + "_HOST12.csv";
      contadores = ["67202894", "67189840", "73410491", "73403763", "67202932"];
    }

    procesarHost(host03, host12, columnasComunes.concat(contadores));
  }
}

async function procesarHost(familiaHost03, familiaHost12, contadores) {
  try {
    const arregloFinalHost03 = [],
      arregloFinalHost12 = [];
    const nombreFamilia = familiaHost03.slice(14, 22);
    const fechaHoraEjecucion = new Date().toLocaleString("es-ES", {
      timeZone: "America/Caracas",
    });
    const rowsHost03 = await leerFilas(familiaHost03, {});
    const rowsHost12 = await leerFilas(familiaHost12, {});
    //console.log(`⏳ Procesando familia ${nombreFamilia} - Fecha: ${fecha} Hora: ${hora} - Inicio: ${fechaHoraEjecucion} \n`)
    let newHeader = [];

    //console.log(rowsHost03);
    for (let y = 1; y < rowsHost03.length; y++) {
      //comienzo desde la fila 1
      let miObjetoHost03 = {};
      for (let x = 0; x < contadores.length; x++) {
        miObjetoHost03[contadores[x]] = rowsHost03[y][contadores[x]];
        //newHeader[x] = {'id': contadores[x], 'title':contadores[x]} //esta comentada para que o incluya la cabecera de este archivo
      }
      //console.log(miObjetoHost03);
      arregloFinalHost03.push(miObjetoHost03);
    }

    for (let y = 0; y < rowsHost12.length; y++) {
      let miObjetoHost12 = {};
      for (let x = 0; x < contadores.length; x++) {
        miObjetoHost12[contadores[x]] = rowsHost12[y][contadores[x]];
        newHeader[x] = { id: contadores[x], title: contadores[x] };
      }
      console.log(miObjetoHost12);
      arregloFinalHost12.push(miObjetoHost12);
    }

    /*********************************************** Escribe la salida en un nuevo archivo csv con sus contadores especificos ******************************************************/
    const csvWriter = createCsvWriter({
      path: "./UNIFICADOS/" + nombreFamilia + ".csv",
      header: newHeader,
    });

    csvWriter
      .writeRecords(arregloFinalHost12.concat(arregloFinalHost03))
      .then(() =>
        console.log(
          `⁙ Creando el archivo de contadores unificado ${nombreFamilia}.csv de la familia ${nombreFamilia} con éxito. - Fin: ${fechaHoraEjecucion} ⁙\n`
        )
      );
    /********************************************************************************************************************************************************************************/
  } catch (error) {
    console.error(`🐞 Error:`, error.message);
  }
}

leerDirectorio_procesar();
