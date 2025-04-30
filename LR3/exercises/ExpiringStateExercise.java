package com.ververica.flinktraining.exercises.datastream_java.process;

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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Упражнение "Истекающее состояние" - обогащение данных о поездках такси информацией о тарифах
 * с обработкой несоответствий и очисткой состояния по таймеру.
 *
 * Основные задачи:
 * 1. Соединить потоки данных о поездках и тарифах по rideId
 * 2. Обработать случаи, когда для поездки нет тарифа или наоборот
 * 3. Автоматически очищать состояние по истечении времени ожидания
 *
 * Особенности реализации:
 * - Использование event-time обработки
 * - Управляемое состояние для хранения непарных событий
 * - Таймеры для очистки устаревшего состояния
 * - Side outputs для сбора несоответствующих событий
 */
public class ExpiringStateExercise extends ExerciseBase {
    // Теги для side outputs (непарные поездки и тарифы)
    static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
    static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

    public static void main(String[] args) throws Exception {
        // Парсинг параметров командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
        final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

        // Настройки временных параметров
        final int maxEventDelay = 60;           // Максимальная задержка событий (секунды)
        final int servingSpeedFactor = 600;     // Ускорение обработки (10 минут данных в секунду)

        // Настройка среды выполнения
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);  // Использование времени событий
        env.setParallelism(ExerciseBase.parallelism);  // Установка параллелизма

        // Создание и обработка потока данных о поездках
        DataStream<TaxiRide> rides = env
                .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
                .filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))  // Фильтрация начальных событий
                .keyBy(ride -> ride.rideId);  // Группировка по ID поездки

        // Создание и обработка потока данных о тарифах
        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
                .keyBy(fare -> fare.rideId);  // Группировка по ID поездки

        // Соединение потоков и обработка с помощью KeyedCoProcessFunction
        SingleOutputStreamOperator processed = rides
                .connect(fares)
                .process(new EnrichmentFunction());  // Основная функция обработки

        // Вывод непарных тарифов (для демонстрации)
        printOrTest(processed.getSideOutput(unmatchedFares));

        // Запуск задания
        env.execute("ExpiringStateSolution (java)");
    }

    /**
     * Функция обогащения, соединяющая поездки и тарифы с обработкой таймаутов.
     *
     * Основная логика:
     * 1. Хранит непарные поездки и тарифы в состоянии
     * 2. При получении пары - создает результат и очищает состояние
     * 3. При таймауте - отправляет непарные события в side outputs
     */
    public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        // Состояние для хранения непарной поездки
        private ValueState<TaxiRide> rideState;
        // Состояние для хранения непарного тарифа
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            // Инициализация состояния для хранения поездок
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            // Инициализация состояния для хранения тарифов
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Обработка события поездки
            TaxiFare fare = fareState.value();
            if (fare != null) {
                // Если есть соответствующий тариф - создаем пару
                fareState.clear();
                // Отменяем таймер для этого тарифа
                context.timerService().deleteEventTimeTimer(fare.getEventTime());
                out.collect(new Tuple2(ride, fare));
            } else {
                // Сохраняем поездку в состоянии
                rideState.update(ride);
                // Устанавливаем таймер для очистки состояния
                context.timerService().registerEventTimeTimer(ride.getEventTime());
            }
        }

        @Override
        public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Обработка события тарифа
            TaxiRide ride = rideState.value();
            if (ride != null) {
                // Если есть соответствующая поездка - создаем пару
                rideState.clear();
                // Отменяем таймер для этой поездки
                context.timerService().deleteEventTimeTimer(ride.getEventTime());
                out.collect(new Tuple2(ride, fare));
            } else {
                // Сохраняем тариф в состоянии
                fareState.update(fare);
                // Устанавливаем таймер для очистки состояния
                context.timerService().registerEventTimeTimer(fare.getEventTime());
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Обработка таймаута - очистка устаревшего состояния
            if (fareState.value() != null) {
                // Отправка непарного тарифа в side output
                ctx.output(unmatchedFares, fareState.value());
                fareState.clear();
            }
            if (rideState.value() != null) {
                // Отправка непарной поездки в side output
                ctx.output(unmatchedRides, rideState.value());
                rideState.clear();
            }
        }
    }
}