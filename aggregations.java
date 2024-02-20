  private Future<Void> actualizaStatusCargasPostgre(JDBCClient client, JsonObject cargas, int statusPostgre) {
    //logger.info("actualizando status de las cargas ");
    Promise<Void> promesa = Promise.promise();
    client.getConnection(res -> {
      if (res.succeeded()) {
        SQLConnection connection = res.result();
        JsonArray dataArray = cargas.getJsonArray("data");
        //int status = sqlResult.getInteger("statusPostgre");

        List<Future> updateFutures = new ArrayList<>();
        //logger.info("status postgre: " + statusPostgre);
        for (int i = 0; i < dataArray.size(); i++) {
          JsonObject item = dataArray.getJsonObject(i);
          Integer idCarga = item.getInteger("id_carga");

          // Construct the SQL query to update the status
          String updateQuery = "UPDATE siippg_cargas SET id_estatus_estadisticas = ? WHERE id_carga = ?";
          //logger.info("updateQuery: " + updateQuery);
          Promise<Void> updatePromise = Promise.promise();
          updateFutures.add(updatePromise.future());

          // Execute the update query
          //
          connection.updateWithParams(updateQuery,  new JsonArray().add(statusPostgre).add(idCarga), updateRes -> {
            if (updateRes.succeeded()) {
              //logger.info("estatus postgre actualiado con exito ");
              updatePromise.complete();
            } else {
              //logger.info("ocurrio un error al actualizar las cargas ");
              updatePromise.fail(updateRes.cause());
            }
          });
        }

        // Wait for all updates to complete
        CompositeFuture.join(updateFutures).onComplete(ar -> {
          connection.close(); // Ensure to close the connection
          if (ar.succeeded()) {
            // Here, instead of creating a new JsonObject, pass sqlResult forward.
            promesa.complete();
          } else {
            promesa.fail(ar.cause());
          }
        });
      } else {
        promesa.fail(res.cause());
      }
    });
    return promesa.future();
  }

  private Future<JsonObject> buscarCurps(JsonObject cargas){
    //BUSCANDO LAS CURPS
    Promise<JsonObject> promesa = Promise.promise();
    //ACOMODAMOS LAS CARGAS POR AÑO
    JsonObject groupedData = groupDataByYear(cargas);
    MongoClient mongoClient = ConexionMongo.getConexionMongo();

    // Initialize a map to hold futures for each year's aggregation
    Map<String, List<Future>> yearAggregationFutures = new HashMap<>();
    JsonObject resultByYear = new JsonObject();

    //RECORREMOS EL ARRAY CON LOS ID DE CARGA ORDENADOS POR AÑO
    for (String year : groupedData.fieldNames()) {
      //SELECCIONAMOS TODOS LOS DATOS DE UN AÑO
      JsonArray dataArray = groupedData.getJsonArray(year);
      //CREAMOS UNA LISTA PARA ALMACENAR LOS FUTUROS DE LAS CARGAS
      List<Future> aggregationFutures = new ArrayList<>();

      //RECORREMOS LAS CARGAS
      for (int i = 0; i < dataArray.size(); i++) {
        JsonObject row = dataArray.getJsonObject(i);
        //System.out.println(row);
        String collectionName = row.getString("coleccion");
        Integer idCarga = row.getInteger("id_carga");
        //System.out.println("Collection Name: " + collectionName + ", ID Carga: " + idCarga); // Detailed logging
        /*if (collectionName != "null") {
          //System.out.println("Skipping aggregation for " + idCarga + " due to null collectionName.");
          continue; // Skip this iteration of the loop
        }*/
        JsonObject matchStage = new JsonObject()
          .put("$match", new JsonObject()
            .put("_idCarga", idCarga));

        JsonObject groupStage = new JsonObject()
          .put("$group", new JsonObject()
            .put("_id", new JsonObject()
              .put("datospersonales", "$datosPersonales"))
            .put("beneficios", new JsonObject()
              .put("$addToSet", new JsonObject()
                .put("datosBeneficio", "$datosBeneficio")
                .put("_idCarga", "$_idCarga")
                .put("periodo", "$periodo"))));

        JsonObject projectStage = new JsonObject()
          .put("$project", new JsonObject()
            .put("beneficios", new JsonObject()
              .put("$map", new JsonObject()
                .put("input", "$beneficios")
                .put("as", "beneficio")
                .put("in", new JsonObject()
                  .put("_idCarga", "$$beneficio._idCarga")
                  .put("entidadFederativa", "$$beneficio.datosBeneficio.cveEntFed")
                  .put("periodo", "$$beneficio.periodo")
                  .put("Ramo", "$$beneficio.datosBeneficio.cveRamo")
                  .put("UR", "$$beneficio.datosBeneficio.cveUr")
                  .put("municipio", "$$beneficio.datosBeneficio.cveMunicipio")
                  .put("localidad", "$$beneficio.datosBeneficio.cveLocalidad")
                  .put("fechaApoyo", "$$beneficio.datosBeneficio.fechaBeneficio")
                  .put("montoApoyo", "$$beneficio.datosBeneficio.cantidadApoyo")))));

        JsonArray aggregationPipeline = new JsonArray()
          .add(matchStage)
          .add(groupStage)
          .add(projectStage);
        //System.out.println("Aggregation Pipeline for " + idCarga + ": " + aggregationPipeline.encodePrettily());

        //CREAMOS LA PROMESA QUE CORRESPONDE A LA OPERACION ACTUAL
        Promise<JsonObject> aggregationPromise = Promise.promise();

        //LA ASIGNAMOS A LA LISTA QUE CONTIENE LA RESOLUCION DE LOS FUTUROS
        aggregationFutures.add(aggregationPromise.future());

        //DECLARAMOS LA LISTA CON LOS RESULTADOS
        List<JsonObject> results = new ArrayList<>();

        //EJECTUAMOS NUESTRA AGREGACION
        mongoClient.aggregate(collectionName, aggregationPipeline).handler(document -> {
          System.out.println("Document: " + document.encode());
          // Process each document, possibly adding to a result set specific to this aggregation
          results.add(document);
        }).exceptionHandler(e -> {
          // If an error occurs during this aggregation, fail the aggregation's promise
          System.out.println("Error during aggregation: " + e.getMessage());
          aggregationPromise.fail(e);
        }).endHandler(v -> {
          System.out.println("Aggregation completed for " + idCarga+" coleccion "+collectionName);
          // Once all documents for this aggregation have been processed, complete the aggregation's promise
          JsonObject result = new JsonObject().put("datos", new JsonArray(results));
          aggregationPromise.complete(result);
        });

      }
      //FIN DEL FOR QUE RECORRE LAS CARGAS
      yearAggregationFutures.put(year, aggregationFutures);
    }
    //FIN DEL FOR POR AÑO
    // After setting up all aggregations, wait for all of them to complete
    List<Future> allFutures = yearAggregationFutures.values().stream()
      .flatMap(List::stream)
      .collect(Collectors.toList());

    CompositeFuture.all(new ArrayList<>(allFutures)).onComplete(ar -> {
      if (ar.succeeded()) {
        // Once all futures succeed, compile results by year
        yearAggregationFutures.forEach((year, futures) -> {
          JsonArray allResultsForYear = new JsonArray();
          futures.forEach(future -> {
            JsonObject futureResult = ((Future<JsonObject>) future).result();
            JsonArray futureData = futureResult.getJsonArray("datos");
            allResultsForYear.addAll(futureData);
          });
          JsonObject yearData = new JsonObject().put("datos", allResultsForYear);
          resultByYear.put(year, yearData); // Adjusted this line

        });
        System.out.println(resultByYear.encodePrettily());
        promesa.complete(new JsonObject().put("resultadosPorAno", resultByYear));
      } else {
        System.out.println("error de algo");
        promesa.fail(ar.cause());
      }

    });

    return promesa.future();
  }

/*CONSOLE OUTPUT
Aggregation completed for 3 coleccion cargaPF202306
Aggregation completed for 6 coleccion cargaPF202306
Aggregation completed for 7 coleccion cargaPF202306
Aggregation completed for 8 coleccion cargaPF202306
Aggregation completed for 27 coleccion cargaPM202308
Aggregation completed for 117 coleccion cargaPM202347
Aggregation completed for 130 coleccion cargaPM202348
Aggregation completed for 65 coleccion cargaPF202347
Aggregation completed for 131 coleccion cargaPM202348
Aggregation completed for 118 coleccion cargaPF202347
Aggregation completed for 119 coleccion cargaPF202347
Aggregation completed for 107 coleccion cargaPF202347
Aggregation completed for 45 coleccion cargaPF202338
Aggregation completed for 81 coleccion cargaPF202338
Aggregation completed for 33 coleccion cargaPF202338
Aggregation completed for 23 coleccion cargaPF202308
Aggregation completed for 17 coleccion cargaPF202308
Aggregation completed for 16 coleccion cargaPF202308
Aggregation completed for 19 coleccion cargaPF202308
Aggregation completed for 26 coleccion cargaPF202308
Aggregation completed for 34 coleccion cargaPF202308
Aggregation completed for 13 coleccion cargaPF202308
Aggregation completed for 67 coleccion cargaPF202308
Aggregation completed for 61 coleccion cargaPF202308
Aggregation completed for 143 coleccion cargaPF202311
{
  "2023" : {
    "datos" : [ ]
  }
}

Process finished with exit code 130

*/
