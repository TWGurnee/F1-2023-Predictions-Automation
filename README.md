# F1 2023 Predictions Automation

![F1 2023 Predictions Automation](https://github.com/TWGurnee/F1-2023-Predictions-Automation)

This repository contains the code and resources for automating F1 2023 predictions.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Introduction

The F1 2023 Predictions Automation project allows you to predict the results for the Formula 1 season 2023. This links to a Google Spreadsheet which would allow you to place your predicted order for the season in, and compare your results to your friends.

Please see the example spreadsheet here: ![Spreadsheet](https://github.com/TWGurnee/F1-2023-Predictions-Automation/tree/main/images/ExampleSpreadsheet.PNG)

## Features

- Automated update of reace results
- Driver's Championship results
- Constructor's Championship results
- Wildcard prediction statistics

## Installation

To use this project locally, follow these steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/TWGurnee/F1-2023-Predictions-Automation.git
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Set up a google sheet, and the google sheet credentials in src/secrets

4. Run the application:

   ```bash
   python weekly_update.py
   ```

## Usage

The F1 2023 Predictions Automation project provides a simple command-line interface for interaction. Once the project is set up and running, you can update the sheet after each week by running the following:

```bash
python update_sheet.py
```

## Contributing

Contributions to this project are always welcome. To contribute, follow these steps:

1. Fork the repository.

2. Create a new branch:

   ```bash
   git checkout -b feature/your-feature
   ```

3. Make your changes and commit them:

   ```bash
   git commit -m 'Add some feature'
   ```

4. Push the changes to your branch:

   ```bash
   git push origin feature/your-feature
   ```

5. Create a pull request.

Please ensure that your contributions adhere to the project's coding guidelines and follow the established coding style.

## License

This project is licensed under the [MIT License](LICENSE).

---

Feel free to explore the repository and contribute to the F1 2023 Predictions Automation project!

If you have any questions or need further assistance, please don't hesitate to reach out.

Enjoy predicting the F1 2023 race results! 🏎️🏁
 
