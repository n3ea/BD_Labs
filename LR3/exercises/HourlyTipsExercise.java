package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Упражнение "Чаевые по часам" - вычисление максимальных чаевых водителей за каждый час.
 *
 * Задачи:
 * 1. Вычислить общую сумму чаевых каждого водителя за каждый час
 * 2. Найти максимальную сумму чаевых среди всех водителей за каждый час
 *
 * Особенности реализации:
 * - Использование event-time обработки
 * - Двухэтапная оконная агрегация:
 *   - Сначала группировка по driverId с суммированием чаевых
 *   - Затем глобальный поиск максимума
 *
 * Параметры:
 * -input путь-к-файлу-с-данными
 */
public class HourlyTipsExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        // Чтение параметров запуска
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToFareData);

        // Настройки временных параметров потока
        final int maxEventDelay = 60;       // Максимальная задержка событий (секунды)
        final int servingSpeedFactor = 600; // Ускорение обработки (10 минут данных в секунду)

        // Настройка среды выполнения Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Использование времени событий (event-time) вместо времени обработки
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Установка параллелизма
        env.setParallelism(ExerciseBase.parallelism);

        // Создание источника данных о тарифах такси
        DataStream<TaxiFare> fares = env.addSource(
            fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

        // Обработка данных в два этапа:
        // 1. Вычисление суммы чаевых для каждого водителя за каждый час
        // 2. Нахождение максимальной суммы чаевых за каждый час
        DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
                // Группировка по идентификатору водителя
                .keyBy((TaxiFare fare) -> fare.driverId)
                // Окно длительностью 1 час
                .timeWindow(Time.hours(1))
                // Вычисление суммы чаевых для каждого водителя
                .process(new AddTips())
                // Глобальное окно для всех водителей длительностью 1 час
                .timeWindowAll(Time.hours(1))
                // Нахождение записи с максимальной суммой чаевых
                .maxBy(2);

        // Вывод результатов
        printOrTest(hourlyMax);

        // Запуск задания
        env.execute("Hourly Tips (java)");
    }

    /**
     * Функция для вычисления суммы чаевых водителя за окно.
     * 
     * Принимает:
     * - Ключ (driverId)
     * - Контекст окна
     * - Итератор событий в окне
     * 
     * Возвращает кортеж:
     * - Временная метка конца окна
     * - Идентификатор водителя
     * - Сумма чаевых за окно
     */
    public static class AddTips extends ProcessWindowFunction<
            TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        @Override
        public void process(
                Long driverId,                   // Ключ - идентификатор водителя
                Context context,                 // Контекст окна
                Iterable<TaxiFare> fares,        // События в окне
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            
            // Вычисление суммы чаевых
            float sumOfTips = 0F;
            for (TaxiFare fare : fares) {
                sumOfTips += fare.tip;
            }
            
            // Создание результата с:
            // - временем окончания окна
            // - идентификатором водителя
            // - общей суммой чаевых
            out.collect(new Tuple3<>(
                context.window().getEnd(), 
                driverId, 
                sumOfTips));
        }
    }
}
