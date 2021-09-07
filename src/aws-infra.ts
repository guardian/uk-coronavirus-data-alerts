import {App, Arn, CfnParameter, Duration, Fn} from "@aws-cdk/core";
import {Runtime} from "@aws-cdk/aws-lambda";
import {Schedule} from "@aws-cdk/aws-events";
import {PolicyStatement} from "@aws-cdk/aws-iam";
import type {GuStackProps} from "@guardian/cdk/lib/constructs/core";
import {GuStack} from "@guardian/cdk/lib/constructs/core";
import {GuScheduledLambda} from "@guardian/cdk/lib/patterns/scheduled-lambda";

function constructLambda(emailType: string, notifyEmailAddresses: CfnParameter, app: string, scope: GuStack) {
    return new GuScheduledLambda(scope, app, {
        app,
        runtime: Runtime.PYTHON_3_8,
        handler: 'main.lambda_handler',
        fileName: `${app}.zip`,
        environment: {
            'NOTIFY_EMAIL_ADDRESSES': notifyEmailAddresses.valueAsString,
            'EMAIL_TYPE': emailType
        },
        monitoringConfiguration: {
            toleratedErrorPercentage: 0,
            snsTopicName: Fn.importValue('AlertSNSTopic-PROD')
        },
        rules: [
            {
                schedule: Schedule.expression('cron(0 8 ? * MON-FRI *)'),
                description: 'Check UK coronavirus data every weekday at 8AM'
            }
        ],
        timeout: Duration.minutes(2)
    });
}

class UKCoronavirusDataAlertsStack extends GuStack {

    constructor(scope: App, id: string, props: GuStackProps) {
        super(scope, id, props);

        const verifiedApp = `uk-coronavirus-data-alerts-verified`;
        const unverifiedApp = `uk-coronavirus-data-alerts-unverified`;

        // Pass these as a parameter to avoid putting email addresses in the open
        const verifiedNotifyEmailAddresses = new CfnParameter(this, 'NotifyEmailAddressesVerified', {
            description: 'Comma-separated list of email addresses to notify for the verified data matching the government dashboard',
            type: 'String',
            default: ''
        });
        const unverifiedNotifyEmailAddresses = new CfnParameter(this, 'NotifyEmailAddressesUnverified', {
            description: 'Comma-separated list of email addresses to notify for the latest (unverified) data',
            type: 'String',
            default: ''
        });

        const lambdas = [constructLambda('VERIFIED', verifiedNotifyEmailAddresses, verifiedApp, this),
            constructLambda('UNVERIFIED', unverifiedNotifyEmailAddresses, unverifiedApp, this)]

        lambdas.forEach(l => {
            l.addToRolePolicy(new PolicyStatement({
                actions: ['s3:GetObject', 's3:PutObject'],
                resources: ['arn:aws:s3:::investigations-data-dev/*']
            }));

            l.addToRolePolicy(new PolicyStatement({
                actions: ['ses:SendEmail'],
                resources: [Arn.format({
                    service: 'ses',
                    resource: 'identity',
                    resourceName: 'investigations.and.reporting@theguardian.com'
                }, this)]
            }));
        })
    }
}

new UKCoronavirusDataAlertsStack(new App(), 'UKCoronavirusDataAlerts', {
    stack: 'pfi-structured'
});