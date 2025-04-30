package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Упражнение "Обогащение данных с состоянием" - соединение данных о поездках такси
 * с информацией о тарифах с использованием stateful-обработки.
 *
 * Основная задача:
 * - Объединить потоки данных TaxiRide и TaxiFare по rideId
 * - Использовать управляемое состояние Flink для хранения промежуточных данных
 * - Обеспечить корректное соединение даже при неупорядоченном поступлении данных
 *
 * Параметры запуска:
 * -rides путь-к-файлу-с-поездками
 * -fares путь-к-файлу-с-тарифами
 */
public class RidesAndFaresExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {
        // Парсинг параметров командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        final String ridesFile = params.get("rides", pathToRideData);
        final String faresFile = params.get("fares", pathToFareData);

        // Настройки временных параметров потока
        final int delay = 60;                   // Максимальная задержка событий (секунды)
        final int servingSpeedFactor = 1800;    // Ускорение обработки (30 минут данных в секунду)

        // Конфигурация среды выполнения с Web UI
        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
        conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(ExerciseBase.parallelism);

        env.enableCheckpointing(10000L);
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Создание потока данных о поездках
        DataStream<TaxiRide> rides = env
                .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor)))
                .filter((TaxiRide ride) -> ride.isStart)  // Фильтрация только начальных событий поездок
                .keyBy(ride -> ride.rideId);              // Ключевание по идентификатору поездки

        // Создание потока данных о тарифах
        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor)))
                .keyBy(fare -> fare.rideId);              // Ключевание по идентификатору поездки

        // Соединение потоков и обогащение данных
        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
                .connect(fares)                           // Соединение двух потоков
                .flatMap(new EnrichmentFunction())        // Применение функции обогащения
                .uid("enrichment");                      // Уникальный идентификатор для State Processor API

        // Вывод результатов
        printOrTest(enrichedRides);

        // Запуск задания
        env.execute("Join Rides with Fares (java RichCoFlatMap)");
    }

    /**
     * Функция обогащения, реализующая соединение по rideId с использованием состояния.
     * 
     * Принцип работы:
     * 1. Хранит последнее полученное событие каждого типа (ride или fare) в состоянии
     * 2. При получении события одного типа проверяет наличие соответствующего события другого типа
     * 3. При нахождении пары генерирует результат и очищает состояние
     */
    public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        // Управляемое состояние для хранения данных о поездке
        private ValueState<TaxiRide> rideState;
        // Управляемое состояние для хранения данных о тарифе
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            // Инициализация состояния для хранения поездки
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            // Инициализация состояния для хранения тарифа
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Обработка события поездки
            TaxiFare fare = fareState.value();
            if (fare != null) {
                // Если есть соответствующий тариф - создаем пару и очищаем состояние
                fareState.clear();
                out.collect(new Tuple2(ride, fare));
            } else {
                // Если тариф еще не получен - сохраняем поездку в состоянии
                rideState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Обработка события тарифа
            TaxiRide ride = rideState.value();
            if (ride != null) {
                // Если есть соответствующая поездка - создаем пару и очищаем состояние
                rideState.clear();
                out.collect(new Tuple2(ride, fare));
            } else {
                // Если поездка еще не получена - сохраняем тариф в состоянии
                fareState.update(fare);
            }
        }
    }
}