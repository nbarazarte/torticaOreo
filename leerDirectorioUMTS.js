/***********************************************************************/
//  Desarrollado por: Coordinación de Estadísticas Operativas (CEO)    *
//  Fecha: 19/02/2023                                                  *
/***********************************************************************/
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const csv = require("csv-parser");
const fs = require("fs");
let allErrors = [];
/*********************************************** Lee las cabeceras (contadores) y filas del archivo de familias ***************************************************************/
function leerCabeceras(file, type, contadores, nombreFamilia, host) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(file)
      .on("error", (error) => {
        reject(error);
      })
      .pipe(csv())
      .on("headers", (headers) => {
        let miFuncion = buscarContadoresFaltantes(
          nombreFamilia,
          contadores,
          headers,
          host
        );

        if (miFuncion.length === 0) {
          resolve(headers);
        }
        reject(
          new Error(
            `Faltan contadores: ${miFuncion} en la familia ${nombreFamilia} del host ${host} - Hora: ${new Date()} \n`
          )
        );
      });
  });
}

function buscarContadoresFaltantes(familia, contadores, head, host) {
  let faltantes = [];
  for (const x of contadores) {
    if (!head.includes(x)) {
      faltantes.push(x);
    }
  }
  return faltantes;
}

function leerDirectorio() {
  const files = fs.readdirSync("./PRUEBAS PYTHON");
  let nombreFamilia, host, contadores;
  const ruta = "./PRUEBAS PYTHON/";
  for (const x of files) {
    nombreFamilia = x.slice(16, -33);
    host = x.slice(0, 6);

    if (nombreFamilia === "50331648") {
      contadores = ["50331745", "50331746"];
      comprobarCrudosCabeceras(ruta + x, contadores, host);
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
      comprobarCrudosCabeceras(ruta + x, contadores, host);
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
      comprobarCrudosCabeceras(ruta + x, contadores, host);
    }

    if (nombreFamilia === "67109368") {
      contadores = ["67179825", "67179826", "67179827", "67179828"];
      comprobarCrudosCabeceras(ruta + x, contadores, host);
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
      comprobarCrudosCabeceras(ruta + x, contadores, host);
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
      comprobarCrudosCabeceras(ruta + x, contadores, host);
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
      comprobarCrudosCabeceras(ruta + x, contadores, host);
    }

    if (nombreFamilia === "67109508") {
      contadores = ["67192690", "67192689"];
      comprobarCrudosCabeceras(ruta + x, contadores, host);
    }

    if (nombreFamilia === "67109391") {
      contadores = ["67192584", "73421766"];
      comprobarCrudosCabeceras(ruta + x, contadores, host);
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
      comprobarCrudosCabeceras(ruta + x, contadores, host);
    }

    if (nombreFamilia === "82834952") {
      contadores = ["73410510", "73410511"];
      comprobarCrudosCabeceras(ruta + x, contadores, host);
    }

    if (nombreFamilia === "82863958") {
      contadores = ["73424897", "73424899"];
      comprobarCrudosCabeceras(ruta + x, contadores, host);
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
      comprobarCrudosCabeceras(ruta + x, contadores, host);
    }

    if (nombreFamilia === "67109390") {
      contadores = ["67202894", "67189840", "73410491", "73403763", "67202932"];
      comprobarCrudosCabeceras(ruta + x, contadores, host);
    }
  }
}

async function comprobarCrudosCabeceras(familia, contadores, host) {
  try {
    const file = familia;
    const nombreFamilia = familia.slice(33, -33);
    const fecha = `${familia.slice(58, -8).split("")[6]}${
      familia.slice(58, -8).split("")[7]
    }/${familia.slice(58, -8).split("")[4]}${
      familia.slice(58, -8).split("")[5]
    }/${familia.slice(58, -8).split("")[0]}${
      familia.slice(58, -8).split("")[1]
    }${familia.slice(58, -8).split("")[2]}${
      familia.slice(58, -8).split("")[3]
    }`;
    const hora = `${familia.slice(66, -4).split("")[0]}${
      familia.slice(66, -4).split("")[1]
    }:${familia.slice(66, -4).split("")[2]}${
      familia.slice(66, -4).split("")[3]
    }`;
    const fechaHoraEjecucion = new Date().toLocaleString("es-ES", {
      timeZone: "America/Caracas",
    });
    await leerCabeceras(file, {}, contadores, nombreFamilia, host); //console.log("Columnas:", columns[4]); //console.log(columns.length);
    console.log(
      `✔ Finalizó la comprobación de los contadores de la familia ${nombreFamilia} de ${host} -Fecha: ${fecha} Hora: ${hora} - Inicio: ${fechaHoraEjecucion} ⁙\n`
    );

    let logError = { errores: "" };
    allErrors.push(logError);

    const csvWriter = createCsvWriter({
      path: "./logUMTS.csv",
      header: [{ id: "errores", title: "Errores" }],
    });

    csvWriter.writeRecords(allErrors).then(); //()=> console.log(`⁙ Creando el archivo de contadores faltantes ⁙`)
  } catch (error) {
    console.error(`🐞 Error:`, error.message);

    let logError = { errores: error.message };
    allErrors.push(logError);

    const csvWriter = createCsvWriter({
      path: "./CONVERTIDOS/logUMTS.csv",
      header: [{ id: "errores", title: "Errores" }],
    });

    csvWriter.writeRecords(allErrors).then(); //()=> console.log(`⁙ Creando el archivo de contadores faltantes ⁙`)
  }
}

leerDirectorio();
