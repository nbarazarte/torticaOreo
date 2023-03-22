/***********************************************************************/
//  Desarrollado por: Coordinación de Estadísticas Operativas (CEO)    *
//  Fecha: 19/02/2023                                                  *
/***********************************************************************/
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
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
  const files = fs.readdirSync("./PRUEBAS PYTHON");
  let nombreFamilia, host, contadores;
  const columnasComunes = [
    "Result Time",
    "Granularity Period",
    "Object Name",
    "Reliability",
  ];
  const ruta = "./PRUEBAS PYTHON/";

  for (const x of files) {
    nombreFamilia = x.slice(16, -33);
    host = x.slice(0, 6);

    if (nombreFamilia === "50331648") {
      contadores = ["50331745", "50331746"];
    }

    if (nombreFamilia === "67109365") {
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

    if (nombreFamilia === "67109372") {
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

    if (nombreFamilia === "67109368") {
      contadores = ["67179825", "67179826", "67179827", "67179828"];
    }

    if (nombreFamilia === "67109471") {
      contadores = [
        "67192486",
        "67203850",
        "67203932",
        "73403761",
        "73410507",
        "73441239",
      ];
    }

    if (nombreFamilia === "67109369") {
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

    if (nombreFamilia === "67109367") {
      contadores = [
        "67190404",
        "67190405",
        "67192608",
        "67192609",
        "67193609",
        "67193610",
      ];
    }

    if (nombreFamilia === "67109508") {
      contadores = ["67192690", "67192689"];
    }

    if (nombreFamilia === "67109391") {
      contadores = ["67192584", "73421766"];
    }

    if (nombreFamilia === "67109373") {
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

    if (nombreFamilia === "82834952") {
      contadores = ["73410510", "73410511"];
    }

    if (nombreFamilia === "82863958") {
      contadores = ["73424897", "73424899"];
    }

    if (nombreFamilia === "67109376") {
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

    if (nombreFamilia === "67109390") {
      contadores = ["67202894", "67189840", "73410491", "73403763", "67202932"];
    }

    procesar(ruta + x, columnasComunes.concat(contadores), host);
  }
}

async function procesar(familia, contadores, host) {
  try {
    const file = familia;
    const arregloFinal = [];
    const nombreFamilia = familia.slice(33, -33);
    const fechaHoraEjecucion = new Date().toLocaleString("es-ES", {
      timeZone: "America/Caracas",
    });
    const rows = await leerFilas(file, {});
    //console.log(`⏳ Procesando familia ${nombreFamilia} - Fecha: ${fecha} Hora: ${hora} - Inicio: ${fechaHoraEjecucion} \n`)
    let newHeader = [];
    for (let y = 0; y < rows.length; y++) {
      let miObjeto = {};
      for (let x = 0; x < contadores.length; x++) {
        miObjeto[contadores[x]] = rows[y][contadores[x]];
        newHeader[x] = { id: contadores[x], title: contadores[x] };
      }
      //console.log(miObjeto);
      arregloFinal.push(miObjeto);
    }
    //console.log(arregloFinal);
    /*********************************************** Escribe la salida en un nuevo archivo csv con sus contadores especificos ******************************************************/
    const csvWriter = createCsvWriter({
      path: "./CONVERTIDOS/" + nombreFamilia + "_" + host + ".csv",
      header: newHeader,
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
      .then(() =>
        console.log(
          `⁙ Creando el archivo de contadores ${nombreFamilia}.csv de la familia ${nombreFamilia} ${host} con éxito. - Fin: ${fechaHoraEjecucion} ⁙\n`
        )
      );
    /********************************************************************************************************************************************************************************/
  } catch (error) {
    console.error(`🐞 Error:`, error.message);
  }
}

leerDirectorio_procesar();
