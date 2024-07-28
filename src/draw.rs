use std::path::PathBuf;

use gnuplot::{AutoOption::Fix, AxesCommon, Figure, PlotOption::Caption};

use crate::SimulationResult;

// Draw the lines
// Parameter: Vec<SimulationResult>
pub fn draw_lines(results: &[SimulationResult], path: PathBuf) {
    let mut fg = Figure::new();

    let width = 1920;
    let height = 1080;

    fg.set_title("Miss ratio curve");
    let axes = fg.axes2d();
    axes.set_x_grid(true)
        .set_y_grid(true)
        // 设置 y 轴范围为 0 到 1
        .set_y_range(Fix(0.0), Fix(1.0));
    for result in results {
        axes.set_x_label("Cache size", &[])
            .set_y_label("Miss ratio", &[])
            .lines(
                result.points.iter().map(|(x, _)| *x),
                result.points.iter().map(|(_, y)| *y),
                &[Caption(result.label.as_str())],
            );
    }
    fg.save_to_png(path, width, height).unwrap();
}
