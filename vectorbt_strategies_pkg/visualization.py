import glob, os, matplotlib.pyplot as plt, pandas as pd, numpy as np
from fpdf import FPDF
from math import pi
from datetime import datetime

def plot_radar(csv_path="strategy_results_summary.csv", top_n=10):
    print("📈 生成雷达图")
    df=pd.read_csv(csv_path,index_col=0).sort_values("total_return",ascending=False).head(top_n)
    labels=list(df.index); values=list(df["total_return"])
    vmax,vmin=max(values),min(values)
    norm=[(v-vmin)/(vmax-vmin+1e-8) for v in values]+[0]
    N=len(labels)
    angles=[n/float(N)*2*pi for n in range(N)]+[0]
    plt.figure(figsize=(7,7));ax=plt.subplot(111,polar=True)
    plt.xticks(angles[:-1],labels,size=8)
    ax.plot(angles,norm,linewidth=2,linestyle="solid",color="C0")
    ax.fill(angles,norm,"C0",alpha=0.3)
    plt.title("前策略收益雷达图"); plt.tight_layout()
    os.makedirs("charts", exist_ok=True); plt.savefig("charts/strategy_radar_chart.png"); plt.close()
    print("✅ 保存 charts/strategy_radar_chart.png")

def export_pdf_report(csv_path="strategy_results_summary.csv", charts_dir="charts", out_pdf="Strategy_Report.pdf"):
    df=pd.read_csv(csv_path,index_col=0).sort_values("total_return",ascending=False)
    best=df.index[0];best_r=df.iloc[0,0]
    pdf=FPDF(); pdf.set_auto_page_break(True,15)
    pdf.add_page()
    pdf.set_font("Arial","B",20); pdf.cell(0,10,"量化策略报告",ln=True,align="C")
    pdf.ln(10); pdf.set_font("Arial","",12)
    pdf.multi_cell(0,8,f"生成时间：{datetime.now():%Y-%m-%d %H:%M:%S}\n最佳策略：{best}（收益 {best_r:.2%}）")
    for img in sorted(glob.glob(f"{charts_dir}/*.png")):
        pdf.add_page(); pdf.set_font("Arial","B",12); pdf.cell(0,10,os.path.basename(img),ln=True,align="C")
        pdf.image(img,x=15,y=30,w=180)
    pdf.output(out_pdf); print(f"✅ PDF 已导出至 {out_pdf}")



