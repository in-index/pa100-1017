import os

from flask import Flask, Blueprint, request, render_template, Response, url_for, redirect, send_from_directory
import pandas as pd
from RunDaily import _daily
import plotly.express as px
import plotly.graph_objs as go
from flask_wtf import FlaskForm
from wtforms import StringField, TextAreaField, SubmitField
from wtforms.validators import DataRequired, Email, Length
import subprocess

app = Flask(__name__)
app.config['SECRET_KEY'] = 'foo'

# Paths to different data directories relative to app.root_path
pa_index_dir = os.path.join(app.root_path, 'output')
data_dir = os.path.join(app.root_path, 'data2')

@app.route('/')  # Default page
def index():
    weight_csv_path = os.path.join(data_dir, 'sector_weights2.csv')
    table_csv_path = os.path.join(pa_index_dir, 'table.csv')
    df_mweight = pd.read_csv(weight_csv_path)
    df = pd.read_csv(table_csv_path)
    #df = pd.read_csv('/srv/paindex-test/pa-index/output/table.csv')
    # make a table using plotly
    table = go.Table(header=dict(values=list(df.columns), fill_color='#502d0e', align='center', font=dict(family='Arial', color='white', size=14), height=40), cells=dict(
        values=[df[col] for col in df.columns], fill_color=[['white', 'lightgray'] * 100], align='left', font=dict(family='Arial', color='black', size=12), height=60))
    # change the colors of the fig
    for i in range(len(table['cells']['values'][0])):
        table['cells']['values'][0][i] = "<b>" + \
            str(table['cells']['values'][0][i]) + "</b>"

    
    # create into a plotly figure
    fig = go.Figure(data=[table], layout=dict(height=500))
    # convert figure into html
    plot_html = fig.to_html(full_html=False)

    #creating a pie chart for the market weights that would run daily
    fig2 = px.pie(df_mweight, names='Sector', values='Market Weight', color_discrete_sequence=px.colors.sequential.RdBu)

    #formatting for the pie chart

    #layout update just adjusting the test size and only shows if hovered on I think
    fig2.update_layout(uniformtext_minsize=12, uniformtext_mode='hide')
    #discarding the legend
    fig2.update_layout(showlegend=False)
    #add hole to the center
    fig2.update_traces(hole=.4)  
    #increase size
    fig2.update_layout(height=500) 
    #converted the pie chart into a csv file
    plot_html2 = fig2.to_html(full_html=False)

    # Create a plotly express line plot
    input_csv_path = os.path.join(data_dir, 'index_value2.csv')
    df = pd.read_csv(input_csv_path)
    # df = pd.read_csv('/srv/paindex-test/data/input.csv')
    # create a line plot
    fig3 = px.line(df, x="Date", y="Index Value")
    # update y axis title
    fig3.update_yaxes(title_text='Index Level')
    # update x axis title
    fig3.update_xaxes(title_text='Date')
    # set the tile color to light gray
    fig3.update_layout(plot_bgcolor='whitesmoke')
    # set color of the line to black
    fig3.update_traces(line_color='#502d0e')
    #update height
    fig3.update_layout(height=600)
    # convert figure into html
    plot_html3 = fig3.to_html(full_html=False)

    # Pass both plot_html and plot_html2 as arguments to the same render_template function
    return render_template("index.html", plot_html=plot_html,plot_html2=plot_html2,plot_html3=plot_html3)


@app.route('/contact', methods=['POST', 'GET'])
def contact():
    form = ContactForm()
    if form.validate_on_submit():
        name = form.name.data
        phone_number = form.phone_number.data
        email = form.email.data
        subject = form.subject.data
        message = form.message.data
        
        # Send email
        sender = "noreply@lehigh.edu"
        recipient = 'inindex@lehigh.edu'
        body = f'Name: {name}\nPhone Number: {phone_number}\nEmail: {email}\nSubject: {subject}\nMessage: {message}'
        subject = f'New message from {name} via pa-index'

        mail_cmd = f'echo "{body}" | mail -s "{subject}" -r {sender} -a "From: {sender}" {recipient}'
        subprocess.call(mail_cmd, shell=True)
        
        return redirect(url_for('index'))
    return render_template("Contact.html", form=form)

@app.route('/reports')
def reports():
    return render_template("Reports.html")

@app.route('/methodology')
def methodology():
    table_csv_path = os.path.join(pa_index_dir, 'table.csv')
    df = pd.read_csv(table_csv_path)
    #df = pd.read_csv('/srv/paindex-test/pa-index/output/table.csv')

    table = go.Table(
        header=dict(values=list(df.columns), fill_color='#502d0e', align='center', font=dict(
            family='Arial', color='white', size=14), height=40),
        cells=dict(values=[df[col] for col in df.columns],
                   fill_color=[['white', 'lightgray'] * 100],
                   align='left', font=dict(family='Arial', color='black', size=12), height=60)
    )

    # change the colors of the fig
    for i in range(len(table['cells']['values'][0])):
        table['cells']['values'][0][i] = "<b>" + \
            str(table['cells']['values'][0][i]) + "</b>"

    # create into a plotly figure
    fig = go.Figure(data=[table], layout=dict(height=500))

    # convert figure into html
    plot_html = fig.to_html(full_html=False)

    return render_template("Methodology.html", plot_html=plot_html)


@app.route('/team')
def team():
    return render_template("Team.html")

@app.route('/pdf/<path:filename>')
def pdf(filename):
    return send_from_directory('Reports', filename)

@app.route("/api", methods=['POST'], endpoint="api")
def active():
    return _daily()


class ContactForm(FlaskForm):
    name = StringField('Name', validators=[DataRequired(), Length(max=50)])
    phone_number = StringField('Phone Number', validators=[DataRequired(), Length(max=20)])
    email = StringField('Email', validators=[DataRequired(), Length(max=50)])
    subject = StringField('Subject', validators=[DataRequired(), Length(max=100)])
    message = TextAreaField('Message', validators=[DataRequired(), Length(max=500)])
    submit = SubmitField('Submit')



if __name__ == '__main__':
   app.run(port=1900, debug = True)
