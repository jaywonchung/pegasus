use colored::*;
use colourado::{ColorPalette, PaletteType};

fn main() {
    println!("=== Test 1: colored crate truecolor (hardcoded RGB) ===");
    println!("{}", "Red (255,0,0)".truecolor(255, 0, 0));
    println!("{}", "Green (0,255,0)".truecolor(0, 255, 0));
    println!("{}", "Blue (0,0,255)".truecolor(0, 0, 255));
    println!("{}", "Orange (255,165,0)".truecolor(255, 165, 0));
    println!("{}", "Purple (128,0,128)".truecolor(128, 0, 128));
    println!();

    println!("=== Test 2: colored crate basic colors ===");
    println!("{}", "Red".red());
    println!("{}", "Green".green());
    println!("{}", "Blue".blue());
    println!("{}", "Yellow".yellow());
    println!("{}", "Magenta".magenta());
    println!();

    println!("=== Test 3: colourado palette (8 colors, Pastel) ===");
    let palette = ColorPalette::new(8, PaletteType::Pastel, false);
    for (i, color) in palette.colors.iter().enumerate() {
        let r = (color.red * 255.0) as u8;
        let g = (color.green * 255.0) as u8;
        let b = (color.blue * 255.0) as u8;
        println!(
            "{} - RGB({:3},{:3},{:3})",
            format!("Color {}", i).truecolor(r, g, b),
            r,
            g,
            b
        );
    }
    println!();

    println!("=== Test 4: colourado palette (8 colors, Random) ===");
    let palette = ColorPalette::new(8, PaletteType::Random, false);
    for (i, color) in palette.colors.iter().enumerate() {
        let r = (color.red * 255.0) as u8;
        let g = (color.green * 255.0) as u8;
        let b = (color.blue * 255.0) as u8;
        println!(
            "{} - RGB({:3},{:3},{:3})",
            format!("Color {}", i).truecolor(r, g, b),
            r,
            g,
            b
        );
    }
    println!();

    println!("=== Test 5: Raw ANSI truecolor escape (bypassing colored crate) ===");
    println!("\x1b[38;2;255;0;0mRaw Red\x1b[0m");
    println!("\x1b[38;2;0;255;0mRaw Green\x1b[0m");
    println!("\x1b[38;2;0;0;255mRaw Blue\x1b[0m");
    println!();

    println!("=== Test 6: Pegasus-style host formatting ===");
    let hosts = vec![
        "localhost (cuda_visible_devices=0)",
        "localhost (cuda_visible_devices=1)",
        "localhost (cuda_visible_devices=2)",
        "localhost (cuda_visible_devices=3)",
        "localhost (cuda_visible_devices=4)",
        "localhost (cuda_visible_devices=5)",
        "localhost (cuda_visible_devices=6)",
        "localhost (cuda_visible_devices=7)",
    ];
    let palette = ColorPalette::new(hosts.len() as u32, PaletteType::Pastel, false);
    for (host, color) in hosts.iter().zip(palette.colors.iter()) {
        let r = (color.red * 255.0) as u8;
        let g = (color.green * 255.0) as u8;
        let b = (color.blue * 255.0) as u8;
        println!("[{}] RGB({:3},{:3},{:3})", host.truecolor(r, g, b), r, g, b);
    }
    println!();

    println!("=== Environment check ===");
    println!("TERM: {:?}", std::env::var("TERM").ok());
    println!("COLORTERM: {:?}", std::env::var("COLORTERM").ok());
    println!("NO_COLOR: {:?}", std::env::var("NO_COLOR").ok());
}
