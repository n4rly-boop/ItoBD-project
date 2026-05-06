"""Create Stage III charts from evaluation.csv.

The script reads output/evaluation.csv and creates charts for model comparison,
training time, and confusion matrices.
"""

import os

import matplotlib.pyplot as plt
import pandas as pd


OUTPUT_DIR = "output"
EVALUATION_PATH = os.path.join(OUTPUT_DIR, "evaluation.csv")


def ensure_output_dir():
    """Create output directory if it does not exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_evaluation():
    """Load Stage III evaluation CSV."""
    if not os.path.exists(EVALUATION_PATH):
        raise FileNotFoundError(
            f"{EVALUATION_PATH} not found. Run scripts/stage3.sh first."
        )

    df = pd.read_csv(EVALUATION_PATH)

    # Sometimes concatenated Spark CSV output may contain repeated headers.
    df = df[df["model"] != "model"]

    numeric_cols = [
        "accuracy",
        "areaUnderPR",
        "areaUnderROC",
        "weightedF1",
        "macroF1",
        "trainingTimeSeconds",
        "tn",
        "fp",
        "fn",
        "tp",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def short_model_name(model_name):
    """Convert long model names into readable labels."""
    mapping = {
        "model1_logistic_regression": "Logistic Regression",
        "model2_random_forest": "Random Forest",
    }
    return mapping.get(model_name, model_name)


def plot_model_metrics(df):
    """Plot comparison of main evaluation metrics."""
    metrics = [
        "areaUnderROC",
        "areaUnderPR",
        "accuracy",
        "weightedF1",
        "macroF1",
    ]

    plot_df = df[["model"] + metrics].copy()
    plot_df["model"] = plot_df["model"].apply(short_model_name)
    plot_df = plot_df.set_index("model")

    ax = plot_df.plot(kind="bar", figsize=(11, 6))
    ax.set_title("Stage III Model Evaluation Metrics")
    ax.set_xlabel("Model")
    ax.set_ylabel("Score")
    ax.set_ylim(0, 1)
    ax.legend(title="Metric", bbox_to_anchor=(1.02, 1), loc="upper left")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "stage3_model_metrics.jpg"), dpi=200)
    plt.close()


def plot_training_time(df):
    """Plot training time in minutes."""
    plot_df = df[["model", "trainingTimeSeconds"]].copy()
    plot_df["model"] = plot_df["model"].apply(short_model_name)
    plot_df["trainingTimeMinutes"] = plot_df["trainingTimeSeconds"] / 60.0

    ax = plot_df.plot(
        x="model",
        y="trainingTimeMinutes",
        kind="bar",
        legend=False,
        figsize=(8, 5),
    )
    ax.set_title("Stage III Model Training Time")
    ax.set_xlabel("Model")
    ax.set_ylabel("Training time, minutes")
    ax.grid(axis="y", alpha=0.3)

    for patch in ax.patches:
        height = patch.get_height()
        ax.text(
            patch.get_x() + patch.get_width() / 2,
            height,
            f"{height:.1f}",
            ha="center",
            va="bottom",
        )

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "stage3_training_time.jpg"), dpi=200)
    plt.close()


def plot_confusion_matrix(row, filename):
    """Plot confusion matrix for one model."""
    matrix = [
        [int(row["tn"]), int(row["fp"])],
        [int(row["fn"]), int(row["tp"])],
    ]

    model_label = short_model_name(row["model"])

    fig, ax = plt.subplots(figsize=(6, 5))
    image = ax.imshow(matrix)

    ax.set_title(f"Confusion Matrix: {model_label}")
    ax.set_xlabel("Predicted label")
    ax.set_ylabel("Actual label")

    ax.set_xticks([0, 1])
    ax.set_yticks([0, 1])
    ax.set_xticklabels(["0: On time", "1: Delayed"])
    ax.set_yticklabels(["0: On time", "1: Delayed"])

    for i in range(2):
        for j in range(2):
            ax.text(
                j,
                i,
                f"{matrix[i][j]:,}",
                ha="center",
                va="center",
                fontsize=12,
            )

    fig.colorbar(image, ax=ax)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, filename), dpi=200)
    plt.close()


def plot_confusion_matrices(df):
    """Create confusion matrix charts for both models."""
    for _, row in df.iterrows():
        if row["model"] == "model1_logistic_regression":
            filename = "stage3_confusion_matrix_model1.jpg"
        elif row["model"] == "model2_random_forest":
            filename = "stage3_confusion_matrix_model2.jpg"
        else:
            filename = f"stage3_confusion_matrix_{row['model']}.jpg"

        plot_confusion_matrix(row, filename)


def main():
    """Create all Stage III charts."""
    ensure_output_dir()
    df = load_evaluation()

    print("Loaded evaluation data:")
    print(df)

    plot_model_metrics(df)
    plot_training_time(df)
    plot_confusion_matrices(df)

    print("Stage III charts created:")
    print(os.path.join(OUTPUT_DIR, "stage3_model_metrics.jpg"))
    print(os.path.join(OUTPUT_DIR, "stage3_training_time.jpg"))
    print(os.path.join(OUTPUT_DIR, "stage3_confusion_matrix_model1.jpg"))
    print(os.path.join(OUTPUT_DIR, "stage3_confusion_matrix_model2.jpg"))


if __name__ == "__main__":
    main()