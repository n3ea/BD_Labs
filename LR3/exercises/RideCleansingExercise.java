package com.ververica.flinktraining.exercises.datastream_java.basics;

// Импорты необходимых классов
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Упражнение "Очистка данных о поездках такси" из курса Flink.
 * 
 * Основная задача: фильтрация потока данных о поездках такси для выбора только тех поездок,
 * которые начались и закончились в пределах Нью-Йорка.
 * 
 * Логика работы:
 * 1. Чтение входных параметров (путь к данным)
 * 2. Настройка временных параметров обработки
 * 3. Инициализация окружения Flink
 * 4. Загрузка и обработка данных
 * 5. Вывод результатов
 */
public class RideCleansingExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {
        // Получение параметров запуска из командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        // Путь к входному файлу (по умолчанию используется путь из ExerciseBase)
        final String input = params.get("input", ExerciseBase.pathToRideData);

        System.out.println("point 0"); // Точка отслеживания выполнения

        // Максимальная задержка событий (в секундах)
        final int maxEventDelay = 60;
        // Коэффициент ускорения обработки (10 минут данных обрабатываются за 1 секунду)
        final int servingSpeedFactor = 600;

        System.out.println("point 1"); // Точка отслеживания выполнения

        // Настройка окружения выполнения Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Установка параллелизма по умолчанию
        env.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);

        System.out.println("point 2"); // Точка отслеживания выполнения

        // Создание источника данных о поездках такси
        DataStream<TaxiRide> rides = env.addSource(
            rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor))
        );

        System.out.println("point 3"); // Точка отслеживания выполнения

        // Фильтрация поездок - остаются только поездки в пределах NYC
        DataStream<TaxiRide> filteredRides = rides
                // Применение фильтра для отбора поездок в NYC
                .filter(new NYCFilter());

        System.out.println("point 4"); // Точка отслеживания выполнения

        // Вывод отфильтрованных результатов (или выполнение теста)
        printOrTest(filteredRides);

        // Запуск выполнения задания
        env.execute("Taxi Ride Cleansing");
    }

    /**
     * Пользовательский фильтр для отбора поездок в пределах Нью-Йорка.
     * Реализует интерфейс FilterFunction<TaxiRide>.
     */
    public static class NYCFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            // Проверка, что начальные и конечные координаты находятся в NYC
            // с помощью утилитного класса GeoUtils
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                    GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
}